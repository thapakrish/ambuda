import hashlib
import json
import logging
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path

from celery import chain, group

from ambuda import database as db
from ambuda.utils.s3 import S3Path
from ambuda.tasks import app
from ambuda.tasks.utils import get_db_session
from ambuda.utils import text_exports
from ambuda.utils.text_exports import (
    BULK_EXPORTS,
    BulkExportConfig,
    BulkExportType,
    ExportConfig,
    ExportType,
    write_cached_xml,
    delete_cached_xml,
    create_or_update_xml_export,
    create_xml_file,
    create_plain_text,
    create_pdf,
    create_epub,
    maybe_create_tokens,
    create_vocab_list,
)
from ambuda.utils.text_utils import text_metadata


EXPORTS = {x.slug_pattern: x for x in text_exports.EXPORTS}


def create_text_export_inner(
    text_id: int, export_key: str, app_environment: str, engine=None
) -> None:
    """NOTE: `engine` is exposed for testing"""
    with get_db_session(app_environment, engine=engine) as (session, q, config_obj):
        text = session.get(db.Text, text_id)
        if not text:
            raise ValueError(f"Text with id {text_id} not found")

        logging.info(f"Creating {export_key} export for {text.slug}")

        export_config = EXPORTS.get(export_key)
        if not export_config:
            raise ValueError(f"Unknown export type: {export_key}")

        needs_xml = export_config.type in (ExportType.PLAIN_TEXT, ExportType.PDF)

        # Download XML if needed, otherwise set to None
        xml_path = None
        if needs_xml:
            xml_slug = f"{text.slug}.xml"
            xml_export = q.text_export(xml_slug)

            if not xml_export:
                raise FileNotFoundError(
                    f"XML export not found for {text.slug}. "
                    "XML must be created before this export type."
                )

            if not xml_export.s3_path:
                raise ValueError(
                    f"XML export for {text.slug} exists but has no S3 path. "
                    "XML creation may have failed or is incomplete."
                )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir_path = Path(temp_dir)

            # Download XML if needed
            if needs_xml:
                xml_path = temp_dir_path / f"{text.slug}.xml"
                xml_s3_path = S3Path.from_path(xml_export.s3_path)
                xml_s3_path.download_file(xml_path)
                logging.info(f"Downloaded XML from {xml_s3_path} to {xml_path}")

            # Create the export file
            output_path = temp_dir_path / export_config.slug(text.slug)

            if export_config.type == ExportType.XML:
                create_xml_file(text, output_path)
            elif export_config.type == ExportType.PLAIN_TEXT:
                assert xml_path
                create_plain_text(text, output_path, xml_path)
            elif export_config.type == ExportType.PDF:
                assert xml_path
                assert export_config.scheme
                create_pdf(
                    text,
                    output_path,
                    config_obj.S3_BUCKET,
                    xml_path,
                    export_config.scheme,
                )
            elif export_config.type == ExportType.EPUB:
                create_epub(text, output_path)
            elif export_config.type == ExportType.TOKENS:
                maybe_create_tokens(text, output_path)
            elif export_config.type == ExportType.VOCAB:
                create_vocab_list(text, output_path)
            else:
                raise ValueError(f"Unsupported export type: {export_key}")

            if not output_path.exists():
                logging.info(f"Did not create {output_path} (no data found)")
                return

            file_size = output_path.stat().st_size

            sha256_hash = hashlib.sha256()
            with open(output_path, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
            checksum = sha256_hash.hexdigest()

            export_slug = export_config.slug(text.slug)
            logging.info(
                f"Created {export_key} export at {output_path} (SHA256: {checksum})"
            )

            s3_path = export_config.s3_path(config_obj.S3_BUCKET, text.slug)
            s3_path.upload_file(output_path)
            logging.info(f"Uploaded {export_key} export to {s3_path}")

            if export_config.type == ExportType.XML:
                write_cached_xml(
                    config_obj.SERVER_FILE_CACHE,
                    text.slug,
                    output_path,
                )

            text_export = q.text_export(export_slug)
            if text_export:
                text_export.s3_path = s3_path.path
                text_export.size = file_size
                text_export.sha256_checksum = checksum
                text_export.updated_at = datetime.now(UTC)
                logging.info(f"Updated existing TextExport: {export_slug}")
            else:
                text_export = db.TextExport(
                    text_id=text_id,
                    slug=export_slug,
                    export_type=export_config.type,
                    s3_path=s3_path.path,
                    size=file_size,
                    sha256_checksum=checksum,
                )
                session.add(text_export)
                logging.info(f"Created new TextExport: {export_slug}")
            session.commit()


@app.task(bind=True)
def create_text_export(self, text_id: int, export_key: str, app_environment: str):
    create_text_export_inner(text_id, export_key, app_environment)


@app.task(bind=True)
def upload_xml_export(self, text_id, text_slug, tei_path, app_environment):
    """Upload a TEI XML file produced by the publish flow to S3."""
    tei = Path(tei_path)
    try:
        with get_db_session(app_environment) as (session, q, cfg):
            create_or_update_xml_export(
                text_id=text_id,
                text_slug=text_slug,
                tei_path=tei,
                s3_bucket=cfg.S3_BUCKET,
                session=session,
                q=q,
                cache_dir=cfg.SERVER_FILE_CACHE,
            )
    finally:
        tei.unlink(missing_ok=True)


def delete_text_export_inner(export_id: int, app_environment: str, engine=None):
    with get_db_session(app_environment, engine=engine) as (session, query, config_obj):
        text_export = session.get(db.TextExport, export_id)
        if not text_export:
            logging.warning(f"TextExport with id {export_id} not found")
            return

        try:
            s3_path = S3Path.from_path(text_export.s3_path)
            try:
                s3_path.delete()
                logging.info(f"Deleted S3 file: {s3_path}")
            except Exception as e:
                logging.warning(f"Could not delete S3 file: {e}")

            if text_export.export_type == ExportType.XML:
                text = session.get(db.Text, text_export.text_id)
                if text:
                    delete_cached_xml(
                        config_obj.SERVER_FILE_CACHE,
                        text.slug,
                    )

            session.delete(text_export)
            session.commit()
            logging.info(f"Deleted TextExport record: {export_id}")

        except Exception as e:
            session.rollback()
            logging.error(f"Error deleting TextExport {export_id}: {e}")
            raise


@app.task(bind=True)
def delete_text_export(self, export_id: int, app_environment: str):
    delete_text_export_inner(export_id, app_environment)


def populate_file_cache_inner(app_environment: str, engine=None):
    """Download all XML exports from S3 and write them to the local file cache."""
    with get_db_session(app_environment, engine=engine) as (session, q, config_obj):
        xml_exports = (
            session.query(db.TextExport)
            .filter(db.TextExport.export_type == ExportType.XML)
            .all()
        )
        logging.info(f"Populating file cache with {len(xml_exports)} XML export(s)")

        for export in xml_exports:
            text = session.get(db.Text, export.text_id)
            if not text or not export.s3_path:
                continue

            try:
                with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as tmp:
                    tmp_path = Path(tmp.name)

                s3_path = S3Path.from_path(export.s3_path)
                s3_path.download_file(tmp_path)
                write_cached_xml(config_obj.SERVER_FILE_CACHE, text.slug, tmp_path)
                logging.info(f"Cached XML for {text.slug}")
            except Exception as e:
                logging.warning(f"Failed to cache XML for {text.slug}: {e}")
            finally:
                tmp_path.unlink(missing_ok=True)


@app.task(bind=True)
def populate_file_cache(self, app_environment: str):
    populate_file_cache_inner(app_environment)


def create_all_exports_for_text(text_id: int, app_environment: str):
    xml_exports = [e for e in text_exports.EXPORTS if e.type == ExportType.XML]
    other_exports = [e for e in text_exports.EXPORTS if e.type != ExportType.XML]

    xml_task = create_text_export.si(
        text_id, xml_exports[0].slug_pattern, app_environment
    )

    other_tasks = [
        create_text_export.si(text_id, e.slug_pattern, app_environment)
        for e in other_exports
    ]

    return chain(
        xml_task,
        group(*other_tasks),
    )


def _get_bulk_export_config(bulk_type: BulkExportType):
    """Look up the BulkExportConfig for a given type."""
    for cfg in BULK_EXPORTS:
        if cfg.type == bulk_type:
            return cfg
    raise ValueError(f"No BulkExportConfig for type: {bulk_type}")


def create_text_archive_inner(app_environment, engine=None):
    """Create a ZIP archive of all texts and upload to S3.

    For each text, the ZIP contains:
    - {slug}.xml — TEI XML (downloaded from S3 if available, otherwise generated)
    - metadata.json — metadata for all included texts
    """
    bulk_config = _get_bulk_export_config(BulkExportType.XML)
    zip_filename = bulk_config.slug

    with get_db_session(app_environment, engine=engine) as (session, q, config_obj):
        texts = session.query(db.Text).all()

        if not texts:
            logging.warning("No texts found for archive")
            return

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir_path = Path(temp_dir)
            metadata = []

            for text in texts:
                xml_out_path = temp_dir_path / f"{text.slug}.xml"

                xml_export = (
                    session.query(db.TextExport)
                    .filter(
                        db.TextExport.text_id == text.id,
                        db.TextExport.export_type == ExportType.XML,
                    )
                    .first()
                )

                if xml_export and xml_export.s3_path:
                    try:
                        s3_path = S3Path.from_path(xml_export.s3_path)
                        s3_path.download_file(xml_out_path)
                        logging.info(f"Downloaded XML for {text.slug} from S3")
                    except Exception as e:
                        logging.warning(
                            f"Failed to download XML for {text.slug} from S3: {e}. "
                            "Falling back to generation."
                        )
                        create_xml_file(text, xml_out_path)
                else:
                    create_xml_file(text, xml_out_path)

                metadata.append(text_metadata(text))

            metadata_path = temp_dir_path / "metadata.json"
            metadata_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False))

            zip_path = temp_dir_path / zip_filename
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for entry in metadata:
                    xml_file = temp_dir_path / f"{entry['slug']}.xml"
                    if xml_file.exists():
                        zf.write(xml_file, xml_file.name)
                zf.write(metadata_path, "metadata.json")

            file_size = zip_path.stat().st_size
            sha256_hash = hashlib.sha256()
            with open(zip_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(chunk)
            checksum = sha256_hash.hexdigest()

            s3_path = bulk_config.s3_path(config_obj.S3_BUCKET)
            s3_path.upload_file(zip_path)
            logging.info(f"Uploaded text archive to {s3_path}")

            bulk_export = (
                session.query(db.BulkExport)
                .filter(db.BulkExport.slug == zip_filename)
                .first()
            )
            if bulk_export:
                bulk_export.s3_path = s3_path.path
                bulk_export.size = file_size
                bulk_export.sha256_checksum = checksum
                bulk_export.updated_at = datetime.now(UTC)
            else:
                bulk_export = db.BulkExport(
                    slug=zip_filename,
                    export_type=bulk_config.type,
                    s3_path=s3_path.path,
                    size=file_size,
                    sha256_checksum=checksum,
                )
                session.add(bulk_export)
            session.commit()


@app.task(bind=True)
def create_text_archive(self, app_environment):
    create_text_archive_inner(app_environment)


def move_text_exports_inner(
    export_ids: list[int],
    old_prefix: str,
    new_prefix: str,
    app_environment: str,
    engine=None,
):
    """Move selected TextExports from one prefix to another.

    For each export, this replaces `old_prefix` with `new_prefix` in both the
    slug and the S3 key, copies the file to the new S3 location, deletes the
    old one, and updates the DB record.
    """
    with get_db_session(app_environment, engine=engine) as (session, q, config_obj):
        moved = 0
        for export_id in export_ids:
            text_export = session.get(db.TextExport, export_id)
            if not text_export:
                logging.warning(f"TextExport {export_id} not found, skipping")
                continue

            old_s3 = S3Path.from_path(text_export.s3_path)
            if old_prefix not in old_s3.key:
                logging.warning(
                    f"TextExport {export_id} s3_path '{text_export.s3_path}' "
                    f"does not contain prefix '{old_prefix}', skipping"
                )
                continue

            new_key = old_s3.key.replace(old_prefix, new_prefix, 1)
            new_s3 = S3Path(old_s3.bucket, new_key)

            # Copy then delete
            try:
                old_s3.copy_to(new_s3)
                old_s3.delete()
                logging.info(f"Moved S3 file: {old_s3} -> {new_s3}")
            except Exception as e:
                logging.warning(
                    f"Could not move S3 file for TextExport {export_id}: {e}"
                )

            # Update disk cache for XML exports
            if text_export.export_type == ExportType.XML:
                text = session.get(db.Text, text_export.text_id)
                if text:
                    delete_cached_xml(config_obj.SERVER_FILE_CACHE, text.slug)

            old_s3_path = text_export.s3_path
            text_export.s3_path = new_s3.path
            text_export.updated_at = datetime.now(UTC)
            moved += 1
            logging.info(
                f"Updated TextExport {export_id} s3_path: "
                f"'{old_s3_path}' -> '{new_s3.path}'"
            )

        session.commit()
        logging.info(f"Moved {moved} of {len(export_ids)} export(s)")


@app.task(bind=True)
def move_text_exports(
    self,
    export_ids: list[int],
    old_prefix: str,
    new_prefix: str,
    app_environment: str,
):
    move_text_exports_inner(export_ids, old_prefix, new_prefix, app_environment)

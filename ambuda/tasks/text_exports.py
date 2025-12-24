import logging
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from celery import chain, group

from ambuda import database as db
from ambuda.s3_utils import S3Path
from ambuda.tasks import app
from ambuda.tasks.utils import get_db_session
from ambuda.utils import text_exports
from ambuda.utils.text_exports import (
    ExportType,
    create_xml_file,
    create_plain_text,
    create_pdf,
    maybe_create_tokens,
)
from pydantic import BaseModel


EXPORTS = {x.type: x for x in text_exports.EXPORTS}


def create_text_export_inner(
    text_id: int, export_type: str, app_environment: str, engine=None
) -> None:
    """NOTE: `engine` is exposed for testing"""
    with get_db_session(app_environment, engine=engine) as (session, q, config_obj):
        text = session.get(db.Text, text_id)
        if not text:
            raise ValueError(f"Text with id {text_id} not found")

        logging.info(f"Creating {export_type} export for {text.slug}")

        export_config = EXPORTS.get(export_type)
        if not export_config:
            raise ValueError(f"Unknown export type: {export_type}")

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
            output_path = temp_dir_path / export_config.slug(text)

            if export_config.type == ExportType.XML:
                create_xml_file(text, output_path)
            elif export_config.type == ExportType.PLAIN_TEXT:
                assert xml_path
                create_plain_text(text, output_path, xml_path)
            elif export_config.type == ExportType.PDF:
                assert xml_path
                create_pdf(text, output_path, config_obj.S3_BUCKET, xml_path)
            elif export_config.type == ExportType.TOKENS:
                maybe_create_tokens(text, output_path)
            else:
                raise ValueError(f"Unknown export type: {export_type}")

            if not output_path.exists():
                logging.info(f"Did not create {output_path} (no data found)")
                return

            file_size = output_path.stat().st_size
            export_slug = export_config.slug(text)
            logging.info(f"Created {export_type} export at {output_path}")

            bucket = config_obj.S3_BUCKET
            key = f"text-exports/{export_slug}"
            s3_path = S3Path(bucket, key)
            s3_path.upload_file(output_path)
            logging.info(f"Uploaded {export_type} export to {s3_path}")

            text_export = q.text_export(export_slug)
            if text_export:
                text_export.s3_path = s3_path.path
                text_export.size = file_size
                text_export.updated_at = datetime.now(UTC)
                logging.info(f"Updated existing TextExport: {export_slug}")
            else:
                text_export = db.TextExport(
                    text_id=text_id,
                    slug=export_slug,
                    export_type=export_type,
                    s3_path=s3_path.path,
                    size=file_size,
                )
                session.add(text_export)
                logging.info(f"Created new TextExport: {export_slug}")
            session.commit()


@app.task(bind=True)
def create_text_export(self, text_id: int, export_type: str, app_environment: str):
    create_text_export_inner(text_id, export_type, app_environment)


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


# Specialized tasks for Celery chains


@app.task(bind=True)
def create_xml_export(self, text_id: int, app_environment: str):
    create_text_export_inner(text_id, text_exports.ExportType.XML, app_environment)


@app.task(bind=True)
def create_txt_export(self, text_id: int, app_environment: str):
    create_text_export_inner(
        text_id, text_exports.ExportType.PLAIN_TEXT, app_environment
    )


@app.task(bind=True)
def create_pdf_export(self, text_id: int, app_environment: str):
    create_text_export_inner(text_id, text_exports.ExportType.PDF, app_environment)


@app.task(bind=True)
def create_tokens_export(self, text_id: int, app_environment: str):
    create_text_export_inner(text_id, text_exports.ExportType.TOKENS, app_environment)


def create_all_exports_for_text(text_id: int, app_environment: str):
    return chain(
        create_xml_export.si(text_id, app_environment),
        group(
            create_txt_export.si(text_id, app_environment),
            create_pdf_export.si(text_id, app_environment),
            create_tokens_export.si(text_id, app_environment),
        ),
    )

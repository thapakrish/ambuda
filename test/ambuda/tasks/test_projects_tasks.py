import tempfile

import fitz

import ambuda.queries as q
import ambuda.tasks.projects as projects
import ambuda.tasks.utils


def _create_sample_pdf(output_path: str, num_pages: int):
    """Create a toy PDF with 10 pages."""
    doc = fitz.open()
    for i in range(1, num_pages + 1):
        page = doc.new_page()
        where = fitz.Point(50, 50)
        page.insert_text(where, f"This is page {i}", fontsize=30)
    doc.save(output_path)


def test_create_project_inner(flask_app, s3_mocks):
    with flask_app.app_context():
        from ambuda.queries import get_engine

        project = q.project("test-cool-project")
        assert project is None

        f = tempfile.NamedTemporaryFile()
        _create_sample_pdf(f.name, num_pages=10)

        # Pass the Flask app's engine to share the same :memory: database
        engine = get_engine()

        projects.create_project_from_local_pdf_inner(
            display_title="Test cool project",
            pdf_path=f.name,
            app_environment=flask_app.config["AMBUDA_ENVIRONMENT"],
            creator_id=1,
            task_status=ambuda.tasks.utils.LocalTaskStatus(),
            engine=engine,
        )

        project = q.project("test-cool-project")
        assert project
        assert len(project.pages) == 10

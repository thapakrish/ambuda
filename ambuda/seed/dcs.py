import subprocess
from pathlib import Path

from sqlalchemy.orm import Session

import ambuda.data_utils as data_utils
from ambuda.seed.utils.data_utils import create_db

REPO = "https://github.com/ambuda-org/dcs.git"
PROJECT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_DIR / "data" / "ambuda-dcs"


class UpdateError(Exception):
    pass


def log(*a):
    print(*a)


def fetch_latest_data():
    """Fetch the latest data from the parse data repo."""

    print(f"Fetch from files from {REPO} to {DATA_DIR}")
    if not DATA_DIR.exists():
        subprocess.run(f"mkdir -p {DATA_DIR}", shell=True)
        subprocess.run(f"git clone --branch=main {REPO} {DATA_DIR}", shell=True)

    subprocess.call("git fetch origin", shell=True, cwd=DATA_DIR)
    subprocess.call("git checkout main", shell=True, cwd=DATA_DIR)
    subprocess.call("git reset --hard origin/main", shell=True, cwd=DATA_DIR)


def add_parse_data(text_slug: str, path: Path):
    engine = create_db()
    with Session(engine) as session:
        try:
            data_utils.add_parse_data(session, text_slug, path)
        except ValueError:
            raise UpdateError()


def run():
    log("Fetching latest data ...")
    fetch_latest_data()

    skipped = []
    for path in DATA_DIR.iterdir():
        if path.suffix == ".txt":
            try:
                add_parse_data(path.stem, path)
                log(f"- Added {path.stem} parse data to the database.")
            except UpdateError:
                log(f"- Skipped {path.stem}.")
                skipped.append(path.stem)

    log("Done.")

    if skipped:
        log("")
        log("The following texts were skipped because we couldn't find them")
        log("in the database:")
        for slug in skipped:
            log(f"- {slug}")
        log("")
        log("To add these texts, run the seed scripts in ambuda/seed/texts.")
        log("Note that the Ramayana and the Mahabharata have their own special")
        log("seed scripts.")
    return True


if __name__ == "__main__":
    run()

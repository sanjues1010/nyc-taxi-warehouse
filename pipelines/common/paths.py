from pathlib import Path

REPO_ROOT = next(p for p in Path(__file__).resolve().parents if (p / ".git").exists())

DATA_ROOT = REPO_ROOT / "data"
INPUT_SAMPLE_DIR = DATA_ROOT / "input_sample"
INPUT_MAIN_DIR = DATA_ROOT / "input_main"
OUTPUT_DIR = DATA_ROOT / "output"


def output_dir(iteration: str, source: str) -> Path:
    return OUTPUT_DIR / iteration / source

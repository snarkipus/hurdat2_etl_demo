from pathlib import Path

from hurdat2_etl.core import ETLPipeline
from hurdat2_etl.extract.extract import Extract
from hurdat2_etl.load.load import Load
from hurdat2_etl.transform.transform import Transform


def main() -> None:
    # Configure paths
    input_path = Path("ref/hurdat2-1851-2023-051124.txt")
    output_path = Path("output/hurdat2.db")

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Initialize pipeline components
    extract = Extract(input_path=input_path)
    transform = Transform()
    load = Load(db_path=output_path)

    # Create and run pipeline
    pipeline = ETLPipeline([extract, transform, load])
    pipeline.run(None)


if __name__ == "__main__":
    main()

#!/usr/bin/env python
"""
Performs basic cleaning on the data and save the results in Weights & Biases
"""
import os
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    logger.info(f'Running basic data cleaning')

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    run = wandb.init(project="nyc_airbnb", group="eda", save_code=True)
    local_path = run.use_artifact("sample.csv:latest").file()

    if os.path.isfile(local_path):
        logger.info(f"Fetching artifact {args.input}")
        df = pd.read_csv(local_path)
    else:
        logger.info(f'Artifact {local_path} not found')
        return

    # Drop outliers
    min_price = args.min_price
    max_price = args.max_price
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()
    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    df.to_csv("clean_sample.csv", index=False)

    logger.info(f'Uploading data file to wandb')
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)

    run.finish()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This steps cleans the data")

    parser.add_argument("--input_artifact", type=str, help="the input artifact", required=True)

    parser.add_argument("--output_artifact", type=str, help="the output_artifact", required=True)

    parser.add_argument("--output_type", type=str, help="the type for the output artifact", required=True)

    parser.add_argument("--output_description", type=str, help="a description for the output artifact", required=True)

    parser.add_argument("--min_price", type=float, help="the minimum price to consider", required=True)

    parser.add_argument("--max_price", type=float, help="the maximum price to consider", required=True)

    args = parser.parse_args()

    go(args)

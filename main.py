import argparse


commands_choice = [
    'ingest',
    'generate_report',
]


def parse_arguments():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='Data Processing CLI Tool - Ingest data and generate reports.')
    parser.add_argument("command_name", type=str, choices=commands_choice,
                        help="Name of the command to be executed")
    parser.add_argument("--data_folder", type=str, default="./src/data",
                        help="Path to the data folder (where the files to ingest are located and where to save the reports).")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    if args.command_name == 'ingest':
        from src.pipelines.ingest import AmazonReviewsIngestion
        instance = AmazonReviewsIngestion(data_folder=args.data_folder)
        instance.main()
    elif args.command_name == 'generate_report':
        from src.pipelines.analyze import AmazonReviewsAnalysis
        analyse_folder = f"{args.data_folder}/analysis_output"
        instance = AmazonReviewsAnalysis(output_dir=analyse_folder)
        report = instance.main()
    else:
        raise ValueError(f"Unknown command: {args.command_name}")

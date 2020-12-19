import argparse

import sagemaker_studio_sparkmagic_lib.sparkmagic as sparkmagic


def connect_to_emr(args):
    sparkmagic.connect_to_emr_cluster(
        cluster_id=args.cluster_id,
        user_name=args.user_name,
        role_arn=args.role_arn,
        # Setting kernel restart as false since when executed from cli, code is executed in bash process
        # and its not possible to restart kernel
        restart_kernel=False,
    )


def main():

    parser = argparse.ArgumentParser(
        description="A command line utility to generate configuration to connect SparkMagic kernel to EMR"
    )
    subparsers = parser.add_subparsers(dest="subcommand")

    build_parser = subparsers.add_parser(
        "connect",
        help="""
        Generates configuration needed to connect to EMR cluster. Tool describes emr cluster configurations
        to get details required to setup configuration files
        """,
    )
    build_parser.add_argument(
        "--cluster-id",
        help="The EMR cluster-id. Configuration will be generated to connect to this cluster",
        required=True,
    )
    build_parser.add_argument(
        "--user-name",
        default="Livy",
        help="User Name that is used when submitting jobs to Livy. Default to 'livy'",
    )
    build_parser.add_argument(
        "--role-arn",
        help=f"Optional role-arn to use when getting cluster details. By default, tool uses user profile execution role",
    )
    build_parser.set_defaults(func=connect_to_emr)

    args, extra_args = parser.parse_known_args()
    if args.subcommand is None:
        parser.print_help()
    else:
        args.func(args)


if __name__ == "__main__":
    main()

import argparse


def updater_command(args: argparse.Namespace) -> None:
    pass


def manager_command(args: argparse.Namespace) -> None:
    pass


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")

    updater_parser = subparsers.add_parser("updater")
    updater_parser.set_defaults(func=updater_command)

    manager_parser = subparsers.add_parser("manager")
    manager_parser.set_defaults(func=manager_command)

    args = parser.parse_args()
    if args.command is None:
        parser.print_help()
    else:
        args.func(args)


if __name__ == "__main__":
    main()

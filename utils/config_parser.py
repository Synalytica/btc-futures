import argparse
import configparser
import sys

from utils.enums import Stage


def collect_configs(argv=None) -> dict:
    """Collect arguments"""
    # Do argv default this way, as doing it in the functional
    # declaration sets it at compile time.
    if argv is None:
        argv = sys.argv
    # Parse any conf_file specification
    # We make this parser with add_help=False so that
    # it doesn't parse -h and print help.
    conf_parser = argparse.ArgumentParser(
        description=__doc__,  # printed with -h/--help
        # Don't mess with format of description
        formatter_class=argparse.RawDescriptionHelpFormatter,
        # Turn on help
        add_help=True,
    )
    conf_parser.add_argument(
        "-c",
        "--config",
        dest="configs",
        action="append",
        default=["configs/default.cfg"],
        help="configuration file paths",
    )
    conf_parser.add_argument(
        "-s",
        "--stage",
        dest="stage",
        type=Stage,
        default=Stage.LIVE,
        help="Stage of the strategy",
    )
    args, remaining_argv = conf_parser.parse_known_args()
    config = configparser.ConfigParser(allow_no_value=True)
    success = config.read(args.configs)
    failed = list(set(args.configs) - set(success))

    def map_value(section: str, k: str):
        if config.get(section, k) in [
            "yes",
            "on",
            "no",
            "off",
            "true",
            "false",
            "1",
            "0",
        ]:
            return config.getboolean(section, k)
        else:
            try:
                return config.getint(section, k)
            except (ValueError, TypeError):
                try:
                    return config.getfloat(section, k)
                except (ValueError, TypeError):
                    return config.get(section, k)

    sections = {
        "default": {k: map_value("DEFAULT", k) for k, v in config.defaults().items()}
    }
    [
        sections.update(
            {section: {k: map_value(section, k) for k in config[section].keys()}}
        )
        for section in config.sections()
    ]

    section = sections[args.stage.upper()]

    # Manual CLI params have highest priority

    remaining_argv_dict = dict(zip(i := iter(remaining_argv), i))
    remaining_argv_dict = {
        k.replace("--", ""): v for k, v in remaining_argv_dict.items()
    }

    section.update(remaining_argv_dict)
    return section, args

"""Handler for stuck and suspended Rucio rules.

This script processes stuck/suspended rules with various handling strategies
based on the error type (missing files, corrupt files, file exists, etc.).
"""

import argparse
import logging
import os
import traceback
from argparse import Namespace
from typing import Tuple

import generate_lock_list
import handle_force_retry
import pandas as pd
from handle_corrupt import HandleCorrupt
from handle_file_exists import HandleFileExists
from handle_missing import HandleMissing
from handle_threshold import HandleBulkInvalidate
from utils import Mode, get_stuck_locks_overview

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s [%(name)s] %(funcName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M",
)
logging.getLogger("gfal2").setLevel(logging.WARNING)

def check_arguments() -> Tuple:
    """Parse and validate command-line arguments.

    Returns:
        Tuple: (mode, dry_run, rse, input_file, error, file_list,
                suspended, quiet, account)
    """

    parser = argparse.ArgumentParser(
        description="Handle stuck and suspended Rucio rules."
    )
    parser.add_argument(
        "running_mode",
        type=Mode,
        help="Running mode: threshold-invalidation, file-exists, force-retry, "
        "possibly-corrupt, possibly-missing, list-generation",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Test the script without performing any forceful action.",
    )
    parser.add_argument(
        "--rse",
        type=str,
        help="Site name on which to tackle stuck/suspended rules.",
    )
    parser.add_argument(
        "--account",
        type=str,
        help="Account which rules will be handled. Default: wmcore_output.",
    )
    parser.add_argument(
        "--input-file",
        type=str,
        help="Input file with rules and locks information to be acted upon.",
    )
    parser.add_argument(
        "--error",
        type=str,
        nargs="?",
        help="Specific error of rules to be handled.",
    )
    parser.add_argument(
        "--file-list",
        action="store_true",
        help="The input file consists of a list of files without rule/rse.",
    )
    parser.add_argument(
        "--suspended",
        action="store_true",
        help="The list of locks will include only SUSPENDED rules.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Quiet mode: do not push logs to OS dashboard.",
    )
    
    args = parser.parse_args()

    # Set default account if not provided
    if args.account is None:
        args.account = "wmcore_output"  # Account for Production Output

    # Validate TEMP_PATH environment variable for corrupt file mode
    if args.running_mode == Mode.POSSIBLY_CORRUPT:
        if os.getenv("TEMP_PATH") is None:
            raise ValueError(
                "Environment variable TEMP_PATH must be set if running in "
                "possibly-corrupt mode."
            )

    _validate_arguments(args)
    
    # Handle input file generation if not provided
    if args.input_file is None and args.running_mode != Mode.LIST_GENERATION:
        _setup_input_file(args)

    return (
        args.running_mode,
        args.dry_run,
        args.rse,
        args.input_file,
        args.error,
        args.file_list,
        args.suspended,
        args.quiet,
        args.account,
    )

def _validate_arguments(args: Namespace) -> None:
    """Validate parsed command-line arguments.

    Args:
        args: Parsed arguments namespace.

    Raises:
        ValueError: If arguments are invalid or conflicting.
    """
    invalid_modes = (
        Mode.FILE_EXISTS,
        Mode.POSSIBLY_CORRUPT,
        Mode.POSSIBLY_MISSING,
    )
    if args.running_mode in invalid_modes and args.error is not None:
        raise ValueError(
            f"The --error argument is not required for "
            f"{', '.join(m.value for m in invalid_modes)} modes."
        )

    modes_without_rse = (
        Mode.OVERVIEW,
        Mode.LIST_GENERATION,
        Mode.PNR_INVALIDATION,
        Mode.FORCE_RETRY,
    )
    if (
        not args.file_list
        and args.rse is None
        and args.error is None
        and args.running_mode not in modes_without_rse
    ):
        raise ValueError(
            "If not in file-list mode, an rse or an error must be provided."
        )

    modes_require_csv = (Mode.FORCE_RETRY, Mode.OVERVIEW, Mode.PNR_INVALIDATION)
    if args.running_mode in modes_require_csv and args.file_list:
        raise ValueError(
            "FORCE_RETRY, OVERVIEW, and PNR_INVALIDATION modes require "
            "a csv file with locks, rule ids and rses."
        )


def _setup_input_file(args: Namespace) -> None:
    """Setup input file, generating it if necessary.

    Args:
        args: Arguments namespace to update with input file path.
    """
    lock_type = "suspended" if args.suspended else "stuck"
    default_path = f"./locks_{lock_type}_rules.csv"

    logger.info(
        "No input file provided, checking for default "
        f"locks_{lock_type}_rules.csv"
    )

    if os.path.exists(default_path):
        args.input_file = default_path
        logger.info(f"Input file used: {default_path}")
    else:
        logger.info(
            f"No input file found in local directory. "
            f"Generating list of {lock_type} locks."
        )
        # Generate list of stuck/suspended rules using rucio client
        df_problematic_lock_list = generate_lock_list.df_stuck_locks(
            suspended=args.suspended, account=args.account, rse=args.rse
        )
        # Save list with lock names and sizes
        df_problematic_lock_list.to_csv(default_path, index=False)
        args.input_file = default_path

def _load_stuck_locks(
    input_file: str, file_list: bool, rse: str = None, mode: Mode = None
) -> Tuple[pd.DataFrame, str]:
    """Load and process stuck locks from input file.

    Args:
        input_file: Path to input CSV file.
        file_list: Whether input is a simple list of files.
        rse: Optional RSE to filter by.
        mode: Optional running mode.

    Returns:
        Tuple of (locks DataFrame, state string).
    """
    state = "STUCK"

    if file_list:
        stuck_locks = pd.read_csv(input_file, sep=",", header=None)
        stuck_locks.columns = ["file_name"]
        stuck_locks = stuck_locks.drop_duplicates()
    elif mode != Mode.LIST_GENERATION:
        state = "SUSPENDED" if "suspended" in input_file else "STUCK"
        stuck_locks = pd.read_csv(input_file, sep=",")
        stuck_locks = stuck_locks.drop_duplicates()

        if rse is not None:
            stuck_locks = stuck_locks[stuck_locks.rse == rse].reset_index(
                drop=True
            )
            num_files = stuck_locks.dropna(subset="file_name").shape[0]
            num_rules = len(stuck_locks.rule_id.unique())
            logger.info(
                f"There are {num_files:,} locks blocking {num_rules} rules "
                f"at destination RSE {rse}."
            )
        else:
            num_files = stuck_locks.dropna(subset="file_name").shape[0]
            num_rules = len(stuck_locks.rule_id.unique())
            logger.info(
                f"There are {num_files:,} locks blocking {num_rules} rules."
            )

        stuck_locks = stuck_locks.dropna(subset="error")
    else:
        stuck_locks = None

    return stuck_locks, state


def _force_retry_if_none_found(stuck_locks: pd.DataFrame, mode: Mode) -> None:
    """Force-retry stuck locks when no problematic files were found for the given mode.

    Args:
        stuck_locks: DataFrame of stuck locks that were investigated.
        mode: Running mode used to select which rules are eligible for retry.
    """
    try:
        n_rule, n_locks = handle_force_retry.update_stuck_locks_rules(stuck_locks, mode=mode)
        logger.info(
            f"Found no lost files ({stuck_locks.shape[0]} were investigated). "
            f"Force-retried {n_rule} rules blocking {n_locks} locks."
        )
    except Exception:
        traceback.print_exc()
        logger.info(
            f"Found no lost files ({stuck_locks.shape[0]} were investigated). "
            "Error force-retrying; check logs."
        )


def _process_list_generation(suspended: bool, account: str, rse: str) -> None:
    """Process list generation mode.

    Args:
        suspended: Whether to generate suspended rules list.
        account: Account to filter by.
        rse: Optional RSE to filter by.
    """
    lock_type = "suspended" if suspended else "stuck"
    df_lock_list = generate_lock_list.df_stuck_locks(
        suspended=suspended, account=account, rse=rse
    )
    output_file = f"./locks_{lock_type}_rules.csv"
    df_lock_list.to_csv(output_file, index=False)
    logger.info(f"Saved {lock_type} locks to {output_file}")


if __name__ == "__main__":
    mode,dry_run,rse,input_file,error,file_list,suspended,quiet,account = check_arguments()

    state = "SUSPENDED" if suspended else "STUCK"

    if mode == Mode.LIST_GENERATION:
        _process_list_generation(suspended, account, rse)

    else:
        stuck_locks, state = _load_stuck_locks(input_file, file_list, rse, mode)

    if mode == Mode.OVERVIEW:
        overview = get_stuck_locks_overview(stuck_locks,rse, output_json="./overview.json")
        logger.info(overview)

    elif mode == Mode.PNR_INVALIDATION:
        stuck_locks = stuck_locks.dropna(subset="file_name")
        handle_bulk_invalidate = HandleBulkInvalidate(stuck_locks, state, account, rse, quiet, dry_run)
    elif mode == Mode.FILE_EXISTS:
        file_exists_handler = HandleFileExists(stuck_locks, rse, state, account, dry_run, quiet)
    elif mode == Mode.POSSIBLY_CORRUPT:

        if file_list:
            corrupt_handler = HandleCorrupt(stuck_locks, rse, state, account, quiet, dry_run)
        else:
            corrupt_handler = HandleCorrupt.from_stuck_locks(stuck_locks, rse, state, account, quiet, dry_run)
            if corrupt_handler.found == 0:
                _force_retry_if_none_found(stuck_locks, mode)

    elif mode == Mode.POSSIBLY_MISSING:

        if file_list:
            missing_handler = HandleMissing(stuck_locks, rse, state, account, quiet, dry_run)
        else:
            missing_handler = HandleMissing.from_stuck_locks(stuck_locks, rse, state, account, quiet, dry_run)
            if missing_handler.found == 0:
                _force_retry_if_none_found(stuck_locks, mode)

    elif mode == Mode.FORCE_RETRY:
        # Force-retry to switch SUSPENDED rules to STUCK state

        if error is not None:
            logger.info(f"Force rules with error '{error}' to switch from SUSPENDED to STUCK state.")
        else:
            logger.info("Force rules with all errors to switch from SUSPENDED to STUCK state.")

        n_rule, n_locks = handle_force_retry.update_stuck_locks_rules(stuck_locks, error)
        logger.info(
            f"Updated {n_rule} rules to STUCK state, "
            f"blocking {n_locks} locks."
        )

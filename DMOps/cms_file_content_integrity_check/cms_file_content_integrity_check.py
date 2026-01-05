#!/usr/bin/env python3
"""
CMSSW-only ROOT/EDM integrity checker with JSON output and multi-file support.

Usage (inside CMSSW environment):
  python3 cms_file_content_integrity_check.py <file_or_txt_list> [options]

Options:
  --skip-edmfileutil    Skip edmFileUtil --ls call
  --skip-fwlite         Skip FWLite event sampling
  --samples N           Number of events to sample (fixed points) with FWLite (default 5).
  --step-size S         Check one event every S events (0 to disable, default 50).
  --per-event-timeout T Timeout per event read (seconds, default 20)
  --verbose             Print detailed logs
"""
import argparse, os, sys, subprocess, signal, json

# ROOT / FWLite imports
try:
    import ROOT
except Exception as e:
    print("ERROR: ROOT import failed. Make sure you're in a cmsenv.", file=sys.stderr)
    print("Exception:", e, file=sys.stderr)
    sys.exit(2)

try:
    from DataFormats.FWLite import Events
    FWLITE_AVAILABLE = True
except Exception:
    FWLITE_AVAILABLE = False

# ------------------------------
# Timeout helper
# ------------------------------
class TimeoutExpired(Exception):
    pass

def _timeout_handler(signum, frame):
    raise TimeoutExpired()

def call_with_timeout(func, timeout, *args, **kwargs):
    if timeout is None or timeout <= 0:
        return func(*args, **kwargs)
    old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
    signal.setitimer(signal.ITIMER_REAL, timeout)
    try:
        result = func(*args, **kwargs)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0) 
        signal.signal(signal.SIGALRM, old_handler)
    return result

# ------------------------------
# Subprocess wrapper
# ------------------------------
def run_cmd(cmd, timeout=None):
    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, encoding='utf-8')
        return proc.returncode, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired:
        return -9, "", f"Command timed out after {timeout}s: {' '.join(cmd)}"
    except Exception as e:
        return -1, "", str(e)

# ------------------------------
# CMSSW env check
# ------------------------------
def check_cmssw_env():
    return 'CMSSW_BASE' in os.environ or 'CMSSW_RELEASE_BASE' in os.environ

# ------------------------------
# ROOT open check
# ------------------------------
def quick_root_open(filename):
    try:
        f = ROOT.TFile.Open(filename)
    except Exception as e:
        return False, None, str(e)
    
    # Return error if file is None (failed open) or IsZombie (corrupt structure)
    if not f or f.IsZombie():
        # Return a concise error message
        error_msg = f"ROOT file is a Zombie or failed to open. Error code: {f.GetErrno()}" if f else "Failed to open file (TFile::Open returned None)."
        return False, None, error_msg
    try:
        keys = [k.GetName() for k in f.GetListOfKeys()]
    except Exception as e:
        keys = []
        print(f"WARNING: Could not read keys from open file {filename}: {e}", file=sys.stderr)

    return True, f, keys

# ------------------------------
# edmFileUtil --ls
# ------------------------------
def edmfileutil_ls(filename, timeout=30):
    rc, out, err = run_cmd(["edmFileUtil", "--ls", filename], timeout=timeout)
    return rc==0, out, err

# ------------------------------
# FWlite sampling
# ------------------------------
def fwlite_sample_events(filename, n_samples=5, per_event_timeout=20, sampling_step=50):
    if not FWLITE_AVAILABLE:
        return False, "FWLite not available"
    
    events = None
    try:
        events = Events(filename)
        total = events.size()
    except Exception as e:
        return False, str(e)
        
    if total == 0:
        return True, []

    # 1. Fixed points (start, end, and n_samples - 2 points in between)
    indices = {0}
    if total > 1:
        indices.add(total-1)
    
    if total > 2:
        # Calculate n_samples - 2 intermediate indices
        for i in range(1, n_samples - 1):
            pos = int((i / (n_samples - 1)) * total)
            indices.add(min(pos, total - 1))

    # 2. Step sampling
    if sampling_step > 0:
        for i in range(0, total, sampling_step):
            indices.add(min(i, total-1))

    indices = sorted(list(indices))

    sampled = []
    it = iter(events)
    idx = 0
    try:
        target_set = set(indices)
        got = set()
        
        while True:
            if idx in target_set:
                
                ev = call_with_timeout(lambda: next(it), per_event_timeout)
                non_fatal_error = False
                
                try:
                    # Accessing event number ensures the data record is read
                    _ = ev.eventAuxiliary().event()
                except Exception:
                    # Non-fatal: EDM object itself might be corrupt, but the record was read.
                    non_fatal_error = True
                    pass
                
                got.add(idx)
                sampled.append(idx)
                
                if got == target_set:
                    return True, (sampled, non_fatal_error)
            else:
                next(it) 
                
            idx += 1
            
    except StopIteration:
        # Reached end of file, potentially successfully
        return True, (sampled, non_fatal_error)
    except TimeoutExpired:
        return False, f"Timeout reading event {idx}. Timeout was {per_event_timeout}s."
    except Exception as e:
        return False, str(e)

# ------------------------------
# Single file integrity check
# ------------------------------
def check_file(filename, skip_edmfileutil=False, skip_fwlite=False, samples=5, sampling_step=50, per_event_timeout=20, full_scan=False, strict=False, verbose=False):
    result = {"file": filename, "strict_mode": strict, "full_scan_mode": full_scan, "integrity": "UNKNOWN",
              "root_open_ok": False, "edm_detected": False, "failed_checks": [], "details": {}}
    
    # --- 1. ROOT Open Check ---
    ok, rootfile, keys_or_err = quick_root_open(filename)
    result["root_open_ok"] = ok
    
    if not ok:
        result["integrity"] = "FAILED"
        result["failed_checks"].append("ROOT_Open")
        result["details"]["root_open_error"] = keys_or_err
        return result

    keys = keys_or_err
    is_edm = any(k in ("Events", "Runs", "LuminosityBlocks") for k in keys)
    result["edm_detected"] = is_edm

    # --- 2. edmFileUtil Check ---
    if not skip_edmfileutil:
        ok2, out, err = edmfileutil_ls(filename)
        
        if not ok2:
            result["integrity"] = "FAILED"
            result["failed_checks"].append("EdmFileUtil_Check")
            result["details"]["edmfileutil_stderr"] = err[:2000] if err else "Error: edmFileUtil failed with no stderr output."
        elif verbose:
            result["details"]["edmfileutil_stdout"] = out[:2000]
    
    # --- 3. FWLite Sampling Check ---
    if is_edm and not skip_fwlite:
        
        if full_scan:
            samples = 0
            sampling_step = 1
            
        ok3, fwlite_info = fwlite_sample_events(filename, n_samples=samples, per_event_timeout=per_event_timeout, sampling_step=sampling_step)

        if not ok3:
            result["integrity"] = "FAILED"
            result["failed_checks"].append("FWLite_Sampling")
            result["details"]["fwlite_error"] = fwlite_info
        else:
            if verbose:
                result["details"]["fwlite_sampled_events"] = fwlite_info

    if strict and result["details"].get("fwlite_non_fatal_error"):
        result["integrity"] = "FAILED (Strict)"
        result["failed_checks"].append("FWLite_NonFatal_Strict")
    elif not result["failed_checks"]:
        result["integrity"] = "PASSED"
    else:
        result["integrity"] = "FAILED"

    try:
        rootfile.Close()
    except Exception:
        pass

    return result

# ------------------------------
# Main
# ------------------------------
def main():
    parser = argparse.ArgumentParser(description="CMSSW-only ROOT/EDM integrity checker (JSON output)")
    parser.add_argument("file_or_list", help="Single ROOT file or text file containing list of ROOT files")
    parser.add_argument("--full-scan", action="store_true", help="Disables sampling and forces iteration over all events.")
    parser.add_argument("--strict", action="store_true", help="Returns FAILED integrity status even for non-fatal errors.")
    parser.add_argument("--skip-edmfileutil", action="store_true")
    parser.add_argument("--skip-fwlite", action="store_true")
    parser.add_argument("--samples", type=int, default=5, help="Number of fixed points to sample with FWLite (default 5).")
    parser.add_argument("--step-size", type=int, default=50, help="Check one event every N events (0 to disable, default 50).")
    parser.add_argument("--per-event-timeout", type=int, default=20)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if not check_cmssw_env():
        print("ERROR: CMSSW environment not detected.", file=sys.stderr)
        sys.exit(2)

    files = []
    if os.path.isfile(args.file_or_list) and args.file_or_list.endswith(".txt"):
        with open(args.file_or_list) as f:
            for line in f:
                line = line.strip()
                if line:
                    files.append(line)
    else:
        if args.file_or_list:
            files = [args.file_or_list]
        else:
            print("ERROR: No file or file list provided.", file=sys.stderr)
            sys.exit(1)


    results = []
    for f in files:
        if args.verbose:
            print(f"\nChecking {f} ...", file=sys.stderr)
        try:
            res = check_file(
                f, skip_edmfileutil=args.skip_edmfileutil, skip_fwlite=args.skip_fwlite,
                samples=args.samples, sampling_step=args.step_size, per_event_timeout=args.per_event_timeout, 
                full_scan=args.full_scan, strict=args.strict, verbose=args.verbose
            )
        except Exception as e:
            res = {
                "file": f, "integrity": "FAILED", "root_open_ok": False,
                "edm_detected": False, "failed_checks": ["Fatal_Script_Error"],
                "details": {"fatal_error": str(e)}
            }
            print(f"FATAL ERROR while processing {f}: {e}", file=sys.stderr)
        results.append(res)
        print(json.dumps(res, indent=2))

    passed = sum(1 for r in results if r["integrity"]=="PASSED")
    failed = len(results) - passed
    print(f"\nSummary: {passed} passed, {failed} failed out of {len(results)} files.", file=sys.stderr)

    # Exit code: 0 if all passed, 1 otherwise
    sys.exit(0 if failed==0 else 1)

if __name__ == "__main__":
    main()
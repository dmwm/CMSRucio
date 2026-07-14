import datetime
from rucio.client.client import Client
import os
os.environ['MPLCONFIGDIR'] = '/tmp/'
from matplotlib.ticker import EngFormatter
import matplotlib.pyplot as plt
import pandas as pd

def get_rse_usage(client, rse):
    usage = list(client.get_rse_usage(rse))
    for item in usage:
        del item["updated_at"]
        del item["rse_id"]
    attr = client.list_rse_attributes(rse)
    limits = client.get_rse_limits(rse)
    
    reaper_info = {
        "source": "reaper",
        "rse": rse,
        "free": limits.get("MinFreeSpace", None),
    }
    for item in usage:
        if item["source"] == attr.get("source_for_used_space", "storage"):
            reaper_info["used"] = item["used"]
        if item["source"] == attr.get("source_for_total_space", "storage"):
            reaper_info["total"] = item["total"]
    usage.append(reaper_info)
    return usage

def get_account_usage(client, account):
    usage = client.get_local_account_usage(account)
    usage = pd.json_normalize(list(usage))
    return pd.DataFrame(
        {
            "files": usage["files"],
            "used": usage["bytes"],
            "rse": usage["rse"],
            "free": usage["bytes_remaining"],
            "total": usage["bytes_limit"],
            "source": account,
        }
    )

#output_basepath = "/eos/user/d/dmtops/www/plots/"
output_basepath = '/tmp/'

client = Client()
ddm_rses = list(client.list_rses("(rse_type=DISK)&(cms_type=real)&(tier<3)&(tier>0)"))
ddm_rses = sorted(item["rse"] for item in ddm_rses)

usages = []
for rse in ddm_rses:
    usages.extend(get_rse_usage(client, rse))
rse_usage = (pd.json_normalize(usages)
             .set_index(["rse", "source"])
             .unstack())
accounts = [
            "transfer_ops",
            "wma_prod",
            "wmcore_transferor",
            "wmcore_output",
            "wmcore_pileup",
            "crab_input"
        ]

account_usages = []
for account in accounts:
    account_usages.append(get_account_usage(client, account))
account_usage = (
    pd.concat(account_usages)
    .set_index(["rse", "source"])
    .unstack()
    .loc[ddm_rses]
)

usage = pd.concat([rse_usage, account_usage], axis=1)
usage.loc["Total"] = usage.sum()

volume = pd.DataFrame(
    {
        "Unavailable": usage["used", "unavailable"].fillna(0),
        "Locked": usage["used", "rucio"]
        - usage["used", "unavailable"].fillna(0)
        - usage["used", "expired"].fillna(0),
        "Dynamic": usage["used", "expired"] - usage["used", "obsolete"],
        "Obsolete": usage["used", "obsolete"],
    }
).fillna(0)

volume_colors = {
    "Unavailable": "violet",
    "Locked": "lightblue",
    "Dynamic": "lightgreen",
    "Obsolete": "tomato",
}

account_colors = {
    "transfer_ops": "orange",
    "wma_prod": "red",
    "wmcore_output": "green",
    "wmcore_transferor": "blue",
    "wmcore_pileup": "cyan",
    "crab_input": "dimgrey"
}
rule_volume = usage["used"].filter(account_colors, axis=1).fillna(0)

formatter = EngFormatter(unit="B")
timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

fig, ax = plt.subplots(figsize=(12.2, 6))
ax.yaxis.set_major_formatter(formatter)
volume.plot.bar(ax=ax, stacked=True, color=volume_colors, width=2.9)
volume.plot.bar(ax=ax, stacked=True, width=0.9)
rule_volume.plot.bar(ax=ax, color=account_colors, width=0.9)
rule_volume.plot.bar(ax=ax, width=0.9)
ax.set_xlabel("RSE")
ax.set_ylabel("Used volume")
ax.legend(title="Source", ncol=3)
fig.tight_layout()
fig.text(0.01, 0.01, timestamp, color="grey")
fig.savefig(output_basepath + "rucio_summary_absolute.pdf")

fig, ax = plt.subplots(figsize=(12.2, 6))
limit = usage["total", "reaper"]
occupancy = usage["used", "reaper"] / limit
target = 1 - usage["free", "reaper"] / limit
volume.divide(limit, axis=0).plot.bar(
    ax=ax, stacked=True, color=volume_colors, width=0.9
)
ax.plot(occupancy, marker=5, color="black", linestyle="none", label="Occupancy")
ax.plot(
    target,
    marker=5,
    color="green",
    fillstyle="none",
    linestyle="none",
    label="Target occupancy",
)
rule_volume.divide(limit, axis=0).plot.bar(
    ax=ax,  color=account_colors, width=0.9
)
ax.set_xlabel("RSE")
ax.set_ylabel("Usage relative to rucio-managed quota")
ax.axhline(1, linestyle="dotted", color="black")
ax.set_ylim(0, 1.5)
ax.legend(title="Source", ncol=3)
fig.tight_layout()
fig.text(0.01, 0.01, timestamp, color="grey")
fig.savefig(output_basepath + "rucio_summary_relative.pdf", bbox_inches="tight")

fig, ax = plt.subplots(figsize=(12.2, 6))
mstransferor = (
    account_usage["used", "wmcore_transferor"]
    / account_usage["total", "wmcore_transferor"]
)
mstransferor.plot.bar(ax=ax)
ax.set_xlabel("RSE")
ax.set_ylabel("MSTransferor usage relative to quota")
ax.axhline(1, linestyle="dotted", color="black")
fig.tight_layout()
fig.text(0.01, 0.01, timestamp, color="grey")
fig.savefig(output_basepath + "rucio_summary_mstransferor.pdf", bbox_inches="tight")

fig, ax = plt.subplots(figsize=(12.2, 6))
wmaprod = account_usage["used", "wma_prod"] / 1e15
wmaprod.plot.bar(ax=ax)
ax.set_xlabel("RSE")
ax.set_ylabel("WMAgent usage [PB]")
fig.tight_layout()
fig.text(0.01, 0.01, timestamp, color="grey")
fig.savefig(output_basepath + "rucio_summary_wma_prod.pdf", bbox_inches="tight")

from pathlib import Path
import subprocess

remote_dir = "root://eosuser.cern.ch//eos/user/d/dmtops/www/plots"

for pdf in Path("/tmp/").glob("*.pdf"):
    subprocess.run(
        [
            "xrdcp",
            "-f",
            str(pdf),
            f"{remote_dir}/{pdf.name}",
        ],
        check=True,
    )
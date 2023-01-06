from __future__ import annotations
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Optional, TypeVar
from typing_extensions import TypeAlias
from simple_parsing import ArgumentParser, field, flag
from simple_parsing.wrappers.field_wrapper import DashVariant
import argparse

import functools as ft
from snakemake.utils import available_cpu_count

SNAKEFILE_CHOICES = [
    "Snakefile",
    "snakefile",
    "workflow/Snakefile",
    "workflow/snakefile",
]

RECOMMENDED_LP_SOLVER = "COIN_CMD"


class RerunTriggers(Enum):
    mtime = "mtime"
    params = "params"
    input = "input"
    software_env = "software-env"
    code = "code"


class Schedulers(Enum):
    ilp = "ilp"
    greedy = "greedy"

    @classmethod
    def recommended_scheduler(cls):
        return cls.ilp if RECOMMENDED_LP_SOLVER in get_ilp_solvers() else cls.greedy


@ft.lru_cache(None)
def get_ilp_solvers() -> list[str]:
    try:
        import pulp

        return pulp.list_solvers(onlyAvailable=True)
    except ImportError:
        # Dummy list for the case that pulp is not available
        # This only happend when building docs.
        return ["COIN_CMD"]


parser = ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    add_option_string_dash_variants=DashVariant.DASH,
)


def optlist(
    alias: str | list[str] | None = None,
    metavar=None,
    nargs: str | None = "*",
    *args,
    **kwargs,
):
    return field(None, *args, alias=alias, metavar=metavar, nargs=nargs, **kwargs)


@dataclass
class Execution:
    """Arguments related to execution"""

    target: list[str] = field(positional=True, default_factory=list)
    """Targets to build. May be rules or files."""

    dryrun: bool = flag(False, alias=["dry-run", "n"])
    """
    Do not execute anything, and display what would be done.
    If you have a very large workflow, use --dry-run --quiet to just
    print a summary of the DAG of jobs.
    """
    cache: list[str] = field(metavar="RULE", default_factory=list)
    """
    Store output files of given rules in a central cache given by the environment
    variable $SNAKEMAKE_OUTPUT_CACHE. Likewise, retrieve output files of the given rules
    from this cache if they have been created before (by anybody writing to the same
    cache), instead of actually executing the rules. Output files are identified by
    hashing all steps, parameters and software stack (conda envs or containers) needed
    to create them.
    """

    snakefile: Path | None = field(
        None,
        alias="s",
        metavar="FILE",
        help=(
            f"""
        The workflow definition in form of a snakefile. Usually, you should not need to
        specify this. By default, Snakemake will search for
        {",".join(f"'{choice}'" for choice in SNAKEFILE_CHOICES)}
        beneath the current working directory, in this order. Only if you definitely want a
        different layout, you need to use this parameter.
        """
        ),
    )

    cores: str | int | None = field(
        None, alias="c", metavar="N", const=available_cpu_count()
    )
    """
    Use at most N CPU cores/jobs in parallel. If N is omitted or 'all', the limit is set
    to the number of available CPU cores. In case of cluster/cloud execution, this
    argument sets the maximum number of cores requested from the cluster or cloud
    scheduler. (See https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html#
    resources-remote-execution for more info) This number is available to rules via
    workflow.cores.
    """

    jobs: str | int | None = field(
        None, alias="j", metavar="N", const=available_cpu_count()
    )
    """
    Use at most N CPU cluster/cloud jobs in parallel. For local execution this is an
    alias for --cores. Note: Set to 'unlimited' in case, this does not play a role.
    """

    local_cores: int = field(available_cpu_count(), metavar="N")
    """
    In cluster/cloud mode, use at most N cores of the host machine in parallel (default:
    number of CPU cores of the host). The cores are used to execute local rules. This
    option is ignored when not in cluster/cloud mode.
    """

    resources: Optional[list[str]] = field(None, "res", metavar="NAME=VAL")
    """
    Define additional resources that shall constrain the scheduling analogously to
    --cores (see above). A resource is defined as a name and an integer value. E.g.
    --resources mem_mb=1000. Rules can use resources by defining the resource keyword,
    e.g. resources: mem_mb=600. If now two rules require 600 of the resource 'mem_mb'
    they won't be run in parallel by the scheduler. In cluster/cloud mode, this argument
    will also constrain the amount of resources requested from the server. (See
    https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html#
    resources-remote-execution for more info)
    """

    set_threads: Optional[list[str]] = field(None, metavar="RULE=THREADS", nargs="+")
    """
    Overwrite thread usage of rules. This allows to fine-tune workflow parallelization.
    In particular, this is helpful to target certain cluster nodes by e.g. shifting a
    rule to use more, or less threads than defined in the workflow. Thereby, THREADS has
    to be a positive integer, and RULE has to be the name of the rule.
    """

    max_threads: int | None = field(None)
    """
    Define a global maximum number of threads available to any rule. Rules requesting
    more threads (via the threads keyword) will have their values reduced to the
    maximum. This can be useful when you want to restrict the maximum number of threads
    without modifying the workflow definition or overwriting rules individually with
    --set-threads.
    """
    set_resources: Optional[list[str]] = field(
        None, metavar="RULE:RESOURCE=VALUE", nargs="+"
    )
    """
    Overwrite resource usage of rules. This allows to fine-tune workflow resources. In
    particular, this is helpful to target certain cluster nodes by e.g. defining a
    certain partition for a rule, or overriding a temporary directory. Thereby, VALUE
    has to be a positive integer or a string, RULE has to be the name of the rule, and
    RESOURCE has to be the name of the resource.
    """

    set_scatter: Optional[list[str]] = field(
        None, metavar="NAME=SCATTERITEMS", nargs="+"
    )
    """
    Overwrite number of scatter items of scattergather processes. This allows to
    fine-tune workflow parallelization. Thereby, SCATTERITEMS has to be a positive
    integer, and NAME has to be the name of the scattergather process defined via a
    scattergather directive in the workflow.
    """

    set_resource_scopes: Optional[list[str]] = field(
        None, metavar="RESOURCE=[global|local]]", nargs="+"
    )
    """
    Overwrite resource scopes. A scope determines how a constraint is reckoned in
    cluster execution. With RESOURCE=local, a constraint applied to RESOURCE using
    --resources will be considered the limit for each group submission. With
    RESOURCE=global, the constraint will apply across all groups cumulatively. By
    default, only `mem_mb` and `disk_mb` are considered local, all other resources are
    global. This may be modified in the snakefile using the `resource_scopes:`
    directive. Note that number of threads, specified via --cores, is always considered
    local. (See https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html#
    resources-remote-execution for more info)
    """

    default_resources: Optional[list[str]] = optlist(
        "--default-res", metavar="NAME=INT"
    )
    """
    Define default values of resources for rules that do not define their own values. In
    addition to plain integers, python expressions over inputsize are allowed (e.g.
    '2*input.size_mb'). The inputsize is the sum of the sizes of all input files of a
    rule. By default, Snakemake assumes a default for mem_mb, disk_mb, and tmpdir (see
    below). This option allows to add further defaults (e.g. account and partition for
    slurm) or to overwrite these default values. The defaults are
    'mem_mb=max(2*input.size_mb, 1000)', 'disk_mb=max(2*input.size_mb, 1000)' (i.e.,
    default disk and mem usage is twice the input file size but at least 1GB), and the
    system temporary directory (as given by $TMPDIR, $TEMP, or $TMP) is used for the
    tmpdir resource. The tmpdir resource is automatically used by shell commands,
    scripts and wrappers to store temporary data (as it is mirrored into $TMPDIR, $TEMP,
    and $TMP for the executed subprocesses). If this argument is not specified at all,
    Snakemake just uses the tmpdir resource as outlined above.
    """

    preemption_default: int | None = field(None)
    """
    A preemptible instance can be requested when using the Google Life Sciences API. If
    you set a --preemption-default, all rules will be subject to the default.
    Specifically, this integer is the number of restart attempts that will be made given
    that the instance is killed unexpectedly. Note that preemptible instances have a
    maximum running time of 24 hours. If you want to set preemptible instances for only
    a subset of rules, use --preemptible-rules instead.
    """

    preemptible_rules: Optional[list[str]] = optlist(nargs="+")
    """
    A preemptible instance can be requested when using the Google Life Sciences API. If
    you want to use these instances for a subset of your rules, you can use
    --preemptible-rules and then specify a list of rule and integer pairs, where each
    integer indicates the number of restarts to use for the rule's instance in the case
    that the instance is terminated unexpectedly. --preemptible-rules can be used in
    combination with --preemption-default, and will take priority. Note that preemptible
    instances have a maximum running time of 24. If you want to apply a consistent
    number of retries across all your rules, use --premption-default instead. Example:
    snakemake --preemption-default 10 --preemptible-rules map_reads=3 call_variants=0
    """

    config: Optional[list[str]] = optlist("C", metavar="KEY=VALUE")
    """
    Set or overwrite values in the workflow config object. The workflow config object is
    accessible as variable config inside the workflow. Default values can be set by
    providing a JSON file (see Documentation).
    """

    configfile: Optional[list[Path]] = optlist("configfiles", metavar="FILE", nargs="+")
    """
    Specify or overwrite the config file of the workflow (see the docs). Values
    specified in JSON or YAML format are available in the global config dictionary
    inside the workflow. Multiple files overwrite each other in the given order. Thereby
    missing keys in previous config files are extended by following configfiles. Note
    that this order also includes a config file defined in the workflow definition
    itself (which will come first).
    """

    envvars: Optional[list[str]] = optlist(metavar="VARNAME", nargs="+")
    """
    Environment variables to pass to cloud jobs.
    """

    directory: Path | None = field(None, alias="-d", metavar="DIR")
    """
    Specify working directory (relative paths in the snakefile will use this as their
    origin).
    """

    touch: bool = flag(False, alias="t")
    """
    Touch output files (mark them up to date without really changing them) instead of
    running their commands. This is used to pretend that the rules were executed, in
    order to fool future invocations of snakemake. Fails if a file does not yet exist.
    Note that this will only touch files that would otherwise be recreated by Snakemake
    (e.g. because their input files are newer). For enforcing a touch, combine this with
    --force, --forceall, or --forcerun. Note however that you loose the provenance
    information when the files have been created in realitiy. Hence, this should be used
    only as a last resort.
    """

    keep_going: bool = flag(False, alias="k")
    """
    Go on with independent jobs if a job fails.
    """

    rerun_triggers: list[RerunTriggers] = field(
        default_factory=lambda: [el.value for el in RerunTriggers])
    """
    Define what triggers the rerunning of a job. By default, all triggers are used,
    which guarantees that results are consistent with the workflow code and
    configuration. If you rather prefer the traditional way of just considering file
    modification dates, use '--rerun-trigger mtime'.
    """

    force: bool = flag(False, alias="f")
    """
    Force the execution of the selected target or the first rule regardless of already
    created output.
    """

    forceall: bool = flag(False, alias="F")
    """
    Force the execution of the selected (or the first) rule and all rules it is
    dependent on regardless of already created output.
    """

    forcerun: Optional[list[str]] = optlist("R", metavar="TARGET")
    """
    Force the re-execution or creation of the given rules or files. Use this option if
    you changed a rule and want to have all its output in your workflow updated.
    """

    prioritize: Optional[list[str]] = optlist("P", metavar="TARGET", nargs="+")
    """
    Tell the scheduler to assign creation of given targets (and all their dependencies)
    highest priority. (EXPERIMENTAL)
    """

    batch: str | None = field(None, metavar="RULE=BATCH/BATCHES")
    """
    Only create the given BATCH of the input files of the given RULE. This can be used
    to iteratively run parts of very large workflows. Only the execution plan of the
    relevant part of the workflow has to be calculated, thereby speeding up DAG
    computation. It is recommended to provide the most suitable rule for batching when
    documenting a workflow. It should be some aggregating rule that would be executed
    only once, and has a large number of input files. For example, it can be a rule that
    aggregates over samples.
    """

    until: Optional[list[str]] = optlist("U", metavar="TARGET", nargs="+")
    """
    Runs the pipeline until it reaches the specified rules or files. Only runs jobs that
    are dependencies of the specified rule or files, does not run sibling DAGs.
    """

    omit_from: Optional[list[str]] = optlist("O", metavar="TARGET", nargs="+")
    """
    Prevent the execution or creation of the given rules or files as well as any rules
    or files that are downstream of these targets in the DAG. Also runs jobs in sibling
    DAGs that are independent of the rules or files specified here.
    """

    rerun_incomplete: bool = flag(False, alias="ri")
    """
    Re-run all jobs the output of which is recognized as incomplete.
    """

    shadow_prefix: Path | None = field(None, metavar="DIR", nargs=None)
    """
    Specify a directory in which the 'shadow' directory is created. If not supplied, the
    value is set to the '.snakemake' directory relative to the working directory.
    """

    scheduler: Schedulers = Schedulers.recommended_scheduler()
    """
    Specifies if jobs are selected by a greedy algorithm or by solving an ilp. The ilp
    scheduler aims to reduce runtime and hdd usage by best possible use of resources.
    """

    wms_monitor: str | None = field(None, nargs=None, metavar="WMS_MONITOR")
    """
    IP and port of workflow management system to monitor the execution of snakemake
    (e.g. http://127.0.0.1:5000) Note that if your service requires an authorization
    token, you must export WMS_MONITOR_TOKEN in the environment.
    """

    wms_monitor_arg: Optional[list[str]] = optlist(metavar="NAME=VALUE")
    """
    If the workflow management service accepts extra arguments, provide. them in key
    value pairs with --wms-monitor-arg. For example, to run an existing workflow using a
    wms monitor, you can provide the pair id=12345 and the arguments will be provided to
    the endpoint to first interact with the workflow
    """

    scheduler_ilp_solver: str = field(RECOMMENDED_LP_SOLVER, choices=get_ilp_solvers())
    """
    Specifies solver to be utilized when selecting ilp-scheduler.
    """

    scheduler_solver_path: Path | None = None
    """
    Set the PATH to search for scheduler solver binaries (internal use only).
    """

    conda_base_path: Path | None = None
    """
    Path of conda base installation (home of conda, mamba, activate) (internal use
    only).
    """

    no_subworkflows: bool = flag(False, alias="nosw")
    """
    Do not evaluate or execute subworkflows.
    """


parser.add_arguments(Execution, dest="execution")
args = parser.parse_args()
print(args)
"""
        profile: = field()
        help=""
                        Name of profile to use for configuring
                        Snakemake. Snakemake will search for a corresponding
                        folder in {} and {}. Alternatively, this can be an
                        absolute or relative path.
                        The profile folder has to contain a file 'config.yaml'.
                        This file can be used to set default values for command
                        line options in YAML format. For example,
                        '--cluster qsub' becomes 'cluster: qsub' in the YAML
                        file. Profiles can be obtained from
                        https://github.com/snakemake-profiles.
                        The profile can also be set via the environment variable $SNAKEMAKE_PROFILE.
                        "".format(
            dirs.site_config_dir, dirs.user_config_dir
        ),
        env_var="SNAKEMAKE_PROFILE",
    )

    """

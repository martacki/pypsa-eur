
import pypsa

from _helpers import configure_logging
from vresutils.benchmark import memory_logger

from prepare_and_solve_subproblem import drop_components
from solve_network import prepare_network, solve_network




if __name__ == "__main__":
    
    if 'snakemake' not in globals():
        from _helpers import mock_snakemake
        snakemake = mock_snakemake('add_electricity')
    configure_logging(snakemake)
    
    ### SOLVE SINGLE COUNTRY; FOR REFERENCE...
    n_fine = pypsa.Network(snakemake.input[0]) #unsolved fine network for a subproblem (read from reference network?)

    n_fine = drop_components(n_fine, snakemake.wildcards.cntry)

    tmpdir = snakemake.config['solving'].get('tmpdir')
    
    if tmpdir is not None:
        Path(tmpdir).mkdir(parents=True, exist_ok=True)
        
    opts = snakemake.wildcards.opts.split('-')
    solve_opts = snakemake.config['solving']['options']

    fn = getattr(snakemake.log, 'memory', None)
    with memory_logger(filename=fn, interval=30.) as mem:
        n_fine = prepare_network(n_fine, solve_opts)
        n_fine = solve_network(n_fine, config=snakemake.config, opts=opts,
                               solver_dir=tmpdir,
                               solver_logfile=snakemake.log.solver)
        n_fine.export_to_netcdf(snakemake.output[0])

    logger.info("Maximum memory usage: {}".format(mem.mem_usage))

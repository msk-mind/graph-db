"""Console script for graph_db."""
import sys
import click


APP_CFG = 'APP_CFG'
DATA_CFG = 'DATA_CFG'

@click.command()
@click.option('-d', '--data_config_file', default=None, type=click.Path(exists=True),
              help="path to yaml file containing data input and output parameters. "
                   "See ./data_config.yaml.template")
def main(args=None):
    """Console script for graph_db."""
    click.echo("Replace this message by putting your code into "
               "graph_db.cli.main")
    click.echo("See click documentation at https://click.palletsprojects.com/")
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover

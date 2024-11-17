import click
from queries import query1, query2, query3
from pyspark import SparkContext, SparkConf

@click.command()
@click.option('--nodes_file_path', prompt='Nodes.tsv filepath', help='ex: hetionet/nodes.tsv', default='hetionet/nodes.tsv')
@click.option('--edges_file_path', prompt='Edges.tsv filepath', help='ex: hetionet/edges.tsv', default='hetionet/edges.tsv')
def cli(nodes_file_path, edges_file_path):
    conf = SparkConf().setAppName("Hetionet")
    sc = SparkContext(conf=conf)
    
    # Load the TSV file into an RDD
    edges_rdd = sc.textFile(edges_file_path)
    nodes_rdd = sc.textFile(nodes_file_path)

    # Skip the header row
    edges_header = edges_rdd.first()
    edges_rdd = edges_rdd.filter(lambda line: line != edges_header)
    nodes_header = nodes_rdd.first()
    nodes_rdd = nodes_rdd.filter(lambda line: line != nodes_header)
        
    while True:
        click.echo("\nChoose an option:")
        click.echo("1. Get top 5 drugs with number of associated genes and diseases")
        click.echo("2. Get top 5 number of diseases with associated drug count")
        click.echo("3. Get drugs with top 5 number of genes")
        click.echo("4. Exit")
        
        choice = click.prompt("Enter your choice", type=int)
        
        if choice == 1:
            results = query1(edges_rdd)
            for drug, (num_genes, num_diseases) in results:
                click.echo(f"Drug: {drug}, # of Genes: {num_genes}, # of Diseases: {num_diseases}")
        
        elif choice == 2:
            results = query2(edges_rdd)
            for num_related_drugs, num_diseases_with_count in results:
                click.echo(f"{num_related_drugs} Drug(s) -> {num_diseases_with_count} Disease(s)")
        
        elif choice == 3:
            results = query3(edges_rdd, nodes_rdd)
            for disease_name, drug_count in results:
                click.echo(f"Disease name: {disease_name} with Drug Count {drug_count}")
        
        elif choice == 4:
            break
        
    sc.stop()

if __name__ == '__main__':
    cli()
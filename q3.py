from pyspark import SparkContext, SparkConf

# Initialize Spark context
conf = SparkConf().setAppName("Hetionet")
sc = SparkContext(conf=conf)

# Path to the edges.tsv file (adjust this path accordingly)
edges_file_path = "hetionet/edges.tsv"
nodes_file_path = "hetionet/nodes.tsv"

# Load the TSV file into an RDD
edges_rdd = sc.textFile(edges_file_path)
nodes_rdd = sc.textFile(nodes_file_path)

# Skip the header row
edges_header = edges_rdd.first()
edges_rdd = edges_rdd.filter(lambda line: line != edges_header)
nodes_header = edges_rdd.first()
nodes_rdd = nodes_rdd.filter(lambda line: line != nodes_header)

# Process the data
def initial_edges_processing_mapper(line):
    parts = line.split("\t")
    source_id = parts[0]
    metaedge = parts[1]
    destination_id = parts[2]
    
    # Check if the source is a drug (starts with 'Compound::')
    compound_gene_edges = ["CuG", "CbG", "CdG"]
    
    if metaedge in compound_gene_edges:
        # (drug, gene)
        return (source_id, destination_id)
    
    return None

def initial_nodes_processing_mapper(line):
    parts = line.split("\t")
    id = parts[0]
    name = parts[1]
    kind = parts[2]
    
    return (id, name)

# convert to pairs of (drug, gene)
# then find distinct. Then, convert them 
# to (drug, 1) pairs
processed_rdd = edges_rdd \
    .map(initial_edges_processing_mapper) \
    .filter(lambda x: x is not None) \
    .distinct() \
    .map((lambda a: (a[0], 1)))
    
aggregated_rdd = processed_rdd.reduceByKey(lambda a, b: a + b) # (id, count)

# Process nodes to get (node_id, name) pairs
processed_nodes_rdd = nodes_rdd \
    .map(initial_nodes_processing_mapper)

# Join the two RDDs on the drug ID and transform to (name, count) pairs
joined_rdd = aggregated_rdd \
    .join(processed_nodes_rdd) \
    .map(lambda x: (x[1][1], x[1][0])) # Transform from (id, (count, name)) to (name, count)


sorted_rdd = joined_rdd.sortBy(lambda x: x[1], ascending=False)
top_5 = sorted_rdd.take(5) # top 5 (id, count)


for disease_name, drug_count in top_5:
    print(f"Disease name: {disease_name} with Drug Count {drug_count}")

sc.stop()
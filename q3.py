from pyspark import SparkContext, SparkConf

# Initialize Spark context
conf = SparkConf().setAppName("Hetionet")
sc = SparkContext(conf=conf)

# Path to the edges.tsv file (adjust this path accordingly)
file_path = "hetionet/edges.tsv"

# Load the TSV file into an RDD
edges_rdd = sc.textFile(file_path)

# Skip the header row
edges_header = edges_rdd.first()
edges_rdd = edges_rdd.filter(lambda line: line != edges_header)


# Process the data
def initial_processing_mapper(line):
    parts = line.split("\t")
    source = parts[0]
    metaedge = parts[1]
    destination = parts[2]
    
    # Check if the source is a drug (starts with 'Compound::')
    compound_gene_edges = ["CuG", "CbG", "CdG"]
    
    if metaedge in compound_gene_edges:
        # (drug, gene)
        return (source, destination)
    
    return None

# convert to pairs of (drug, gene)
# then find distinct. Then, convert them 
# to (drug, 1) pairs
processed_rdd = edges_rdd \
    .map(initial_processing_mapper) \
    .filter(lambda x: x is not None) \
    .distinct() \
    .map((lambda a: (a[0], 1)))
    
aggregated_rdd = processed_rdd.reduceByKey(lambda a, b: a + b) # (id, count)
sorted_rdd = aggregated_rdd.sortBy(lambda x: x[1], ascending=False)
top_5 = sorted_rdd.take(5) # top 5 (id, count)

# now we need to do the merging

for disease_id, drug_count in top_5:
    print(f"Disease ID: {disease_id} with Drug Count {drug_count}")

sc.stop()
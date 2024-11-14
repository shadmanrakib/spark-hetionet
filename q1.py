from pyspark import SparkContext, SparkConf

# Initialize Spark context
conf = SparkConf().setAppName("Hetionet")
sc = SparkContext(conf=conf)

# Path to the edges.tsv file (adjust this path accordingly)
file_path = "hetionet/edges.tsv"

# Load the TSV file into an RDD
rdd = sc.textFile(file_path)

# Skip the header row
header = rdd.first()
rdd = rdd.filter(lambda line: line != header)

# Process the data
def initial_processing_mapper(line):
    parts = line.split("\t")
    source = parts[0]
    metaedge = parts[1]
    destination = parts[2]
    
    # Check if the source is a drug (starts with 'Compound::')
    compound_gene_edges = ["CuG", "CbG", "CdG"]
    compound_treatment_edges = ["CtD", "CpD"];
    
    gene_count = 1 if metaedge in compound_gene_edges else 0;
    disease_count = 1 if metaedge in compound_treatment_edges else 0;
    
    return (source, (destination, gene_count, disease_count))

# Apply the transformation to process the lines
# filter out edges that are not between compounds 
# and genes or compounds and diseases
# then remove duplicates since two nodes may have
# multiple edges
# then remove destination node since its no longer needed
# as we already used it for distinct
processed_rdd = rdd \
    .map(initial_processing_mapper) \
    .filter(lambda x: (x[1][1], x[1][2]) != (0, 0)) \
    .distinct() \
    .map((lambda a: (a[0], (a[1][1], a[1][2]))))

# Aggregate by key (drug) to sum the number of genes and diseases
aggregated_rdd = processed_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Sort by the number of genes (descending)
sorted_rdd = aggregated_rdd.sortBy(lambda x: x[1][0], ascending=False)

# Get the top 5 drugs with the most associated genes
top_5 = sorted_rdd.take(5)

# Print the result
for drug, (num_genes, num_diseases) in top_5:
    print(f"Drug: {drug}, Genes: {num_genes}, Diseases: {num_diseases}")

# Stop the Spark context
sc.stop()

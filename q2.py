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
    compound_disease_edges = ["CtD", "CpD"];
    
    if metaedge in compound_disease_edges:
        # (disease, drug)
        return (destination, source)
    
    return None

# create (disease, drug) pairs
# ensure they are distinct
# map to (disease, 1)
processed_rdd = rdd \
    .map(initial_processing_mapper) \
    .filter(lambda x: x is not None) \
    .distinct() \
    .map((lambda a: (a[0], 1)))

# Aggregate by key (drug) to sum the number of genes and diseases
aggregated_num_drugs_per_drug_rdd = processed_rdd.reduceByKey(lambda a, b: a + b)

# at this point we have pairs (disease_id, num related drugs)
# we need to now swap it to be (num_related_drugs, 1) so we can aggregate
# again
processed_num_drugs_count_pairs = aggregated_num_drugs_per_drug_rdd.map(lambda x: (x[1], 1))

# we need to reduce by count to get (num_related_drugs, number of diseases with count)
aggregated_num_diseases_with_num_drugs = processed_num_drugs_count_pairs.reduceByKey(lambda a, b: a + b)

# Sort by the number of genes (descending)
sorted_rdd = aggregated_num_diseases_with_num_drugs.sortBy(lambda x: x[1], ascending=False)

# Get the top 5 drugs with the most associated genes
top_5 = sorted_rdd.take(5)

# Print the result
for num_related_drugs, num_diseases_with_count in top_5:
    print(f"Num of Diseases: {num_diseases_with_count} with Gene Count {num_related_drugs}")

# Stop the Spark context
sc.stop()
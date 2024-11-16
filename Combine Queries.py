from pyspark import SparkContext, SparkConf

# Initialize Spark context
conf = SparkConf().setAppName("Hetionet")
sc = SparkContext(conf=conf)

# Path to the edges.tsv file (adjust this path accordingly)
edges_file_path = "hetionet/edges.tsv"
nodes_file_path = "hetionet/nodes.tsv"


# Load the TSV file into an RDD
# Load the TSV file into an RDD
edges_rdd = sc.textFile(edges_file_path)
nodes_rdd = sc.textFile(nodes_file_path)

# Skip the header row
edges_header = edges_rdd.first()
edges_rdd = edges_rdd.filter(lambda line: line != edges_header)
nodes_header = edges_rdd.first()
nodes_rdd = nodes_rdd.filter(lambda line: line != nodes_header)


######  Query 1  ######
# Process the data
def initial_processing_mapper_q1(line):
    parts = line.split("\t")
    source = parts[0]
    metaedge = parts[1]
    destination = parts[2]
    
    # Check if the source is a drug (starts with 'Compound::')
    compound_gene_edges = ["CuG", "CbG", "CdG"]
    compound_disease_edges = ["CtD", "CpD"];
    
    gene_count = 1 if metaedge in compound_gene_edges else 0;
    disease_count = 1 if metaedge in compound_disease_edges else 0;
    
    return (source, (destination, gene_count, disease_count))

def query1():
    # Apply the transformation to process the lines
    # Filter out edges that are not between compounds 
    # And genes or compounds and diseases
    # Then remove duplicates since two nodes may have multiple edges
    # After remove destination node since its no longer needed as we already used it for distinct
    processed_rdd = edges_rdd \
    .map(initial_processing_mapper_q1) \
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


######  Query 2  ######
# Process the data
def initial_processing_mapper_q2(line):
    parts = line.split("\t")
    source = parts[0]
    metaedge = parts[1]
    destination = parts[2]
    
    # Relationships of Drug to Disease 
    compound_disease_edges = ["CtD", "CpD"];
    
    # Checks if the metaedge is either CtD or CpD
    if metaedge in compound_disease_edges:
        # (disease, drug)
        return (destination, source)
    
    return None

def query2():
    # Create (disease, drug) pairs
    # Ensure they are distinct
    # Map to (disease, 1)
    processed_rdd = edges_rdd \
        .map(initial_processing_mapper_q2) \
        .filter(lambda x: x is not None) \
        .distinct() \
        .map((lambda a: (a[0], 1)))

    # Aggregate by key (drug) to sum the number of genes and diseases
    aggregated_num_drugs_per_drug_rdd = processed_rdd.reduceByKey(lambda a, b: a + b)

    # At this point we have pairs (disease_id, num related drugs)
    # We need to now swap it to be (num_related_drugs, 1) so we can aggregate again
    processed_num_drugs_count_pairs = aggregated_num_drugs_per_drug_rdd.map(lambda x: (x[1], 1))

    # We need to reduce by count to get (num_related_drugs, number of diseases with count)
    aggregated_num_diseases_with_num_drugs = processed_num_drugs_count_pairs.reduceByKey(lambda a, b: a + b)

    # Sort by the number of genes (descending)
    sorted_rdd = aggregated_num_diseases_with_num_drugs.sortBy(lambda x: x[1], ascending=False)

    # Get the top 5 drugs with the most associated genes
    top_5 = sorted_rdd.take(5)

    # Print the results
    for num_related_drugs, num_diseases_with_count in top_5:
        if (num_related_drugs == 1 & num_diseases_with_count == 1):
            print(f"{num_related_drugs} Drug -> {num_diseases_with_count} Disease")

        elif (num_related_drugs == 1 & num_diseases_with_count > 1):
            print(f"{num_related_drugs} Drug -> {num_diseases_with_count} Diseases")

        else:
            print(f"{num_related_drugs} Drugs -> {num_diseases_with_count} Diseases")



######  Query 3  ######

# Process the data
def initial_edges_processing_mapper_q3(line):
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


def initial_nodes_processing_mapper_q3(line):
    parts = line.split("\t")
    id = parts[0]
    name = parts[1]
    kind = parts[2]
    
    return (id, name)


def query3():
    # Convert to pairs of (drug, gene)
    # Then find distinct. Then, convert them 
    # To (drug, 1) pairs
    processed_rdd = edges_rdd \
        .map(initial_edges_processing_mapper_q3) \
        .filter(lambda x: x is not None) \
        .distinct() \
        .map((lambda a: (a[0], 1)))
        
    aggregated_rdd = processed_rdd.reduceByKey(lambda a, b: a + b) # (id, count)

    # Process nodes to get (node_id, name) pairs
    processed_nodes_rdd = nodes_rdd \
        .map(initial_nodes_processing_mapper_q3)

    # Join the two RDDs on the drug ID and transform to (name, count) pairs
    joined_rdd = aggregated_rdd \
        .join(processed_nodes_rdd) \
        .map(lambda x: (x[1][1], x[1][0])) # Transform from (id, (count, name)) to (name, count)


    sorted_rdd = joined_rdd.sortBy(lambda x: x[1], ascending=False)
    top_5 = sorted_rdd.take(5) # Top 5 (id, count)


    for disease_name, drug_count in top_5:
        print(f"Disease name: {disease_name} with Drug Count {drug_count}")


# User's Query Selection 
def query_selection():
    choice = input( "Enter 1 for Query 1, Enter 2 for Query 2, Enter 3 for Query 3: ")

    # Selects Query 1
    if choice == '1':
        query1()
        

    # Selects Query 2
    elif choice == '2':
        query2()
        
    # Selects Query 3
    elif choice == '3':
        query3()   

    else:
        print("Invalid choice. Please try again.")
        query_selection()



def main():
    query_selection()

if __name__ == "__main__":
    main()

######  Query 1  ######
# Process the data
def initial_processing_mapper_q1(line):
    parts = line.split("\t")
    source = parts[0]
    metaedge = parts[1]
    destination = parts[2]
    
    # Check if the source is a drug and goes to gene or disease
    compound_gene_edges = ["CuG", "CbG", "CdG"]
    compound_disease_edges = ["CtD", "CpD"]
    
    gene_count = 1 if metaedge in compound_gene_edges else 0
    disease_count = 1 if metaedge in compound_disease_edges else 0
    
    return (source, (destination, gene_count, disease_count))

def query1(edges_rdd):
    # Apply the transformation to process the lines
    # Filter out edges that are not between compounds 
    # And genes or compounds and diseases
    # Then remove duplicates since two nodes may have multiple edges
    # After remove destination node since its no longer needed as we already used it for distinct
    processed_rdd = edges_rdd \
        .map(initial_processing_mapper_q1) \
        .filter(lambda x: (x[1][1], x[1][2]) != (0, 0)) \
        .distinct() \
        .map(lambda a: (a[0], (a[1][1], a[1][2])))

    # Aggregate by key (drug) to sum the number of genes and diseases
    aggregated_rdd = processed_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # Sort by the number of genes (descending)
    sorted_rdd = aggregated_rdd.sortBy(lambda x: x[1][0], ascending=False)

    # Get the top 5 drugs with the most associated genes
    return sorted_rdd.take(5)

######  Query 2  ######
# Process the data
def initial_processing_mapper_q2(line):
    parts = line.split("\t")
    source = parts[0]
    metaedge = parts[1]
    destination = parts[2]
    
    # Relationships of Drug to Disease 
    compound_disease_edges = ["CtD", "CpD"]
    
    # Checks if the metaedge is either CtD or CpD
    if metaedge in compound_disease_edges:
        # (disease, drug)
        return (destination, source)
    
    return None

def query2(edges_rdd):
    # Create (disease, drug) pairs
    # Ensure they are distinct
    # Map to (disease, 1)
    processed_rdd = edges_rdd \
        .map(initial_processing_mapper_q2) \
        .filter(lambda x: x is not None) \
        .distinct() \
        .map(lambda a: (a[0], 1))

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
    return sorted_rdd.take(5)

######  Query 3  ######
# Process the data
def initial_edges_processing_mapper_q3(line):
    parts = line.split("\t")
    source_id = parts[0]
    metaedge = parts[1]
    destination_id = parts[2]
    
    # Check if the source is a drug and goes to gene
    compound_gene_edges = ["CuG", "CbG", "CdG"]
    
    if metaedge in compound_gene_edges:
        # (drug, gene)
        return (source_id, destination_id)
    
    return None

def initial_nodes_processing_mapper_q3(line):
    parts = line.split("\t")
    
    id = parts[0]
    name = parts[1]
    
    # return (id, name) pair
    return (id, name)

def query3(edges_rdd, nodes_rdd):
    # Convert to pairs of (drug, gene)
    # Then find distinct. Then, convert them 
    # To (drug, 1) pairs
    processed_rdd = edges_rdd \
        .map(initial_edges_processing_mapper_q3) \
        .filter(lambda x: x is not None) \
        .distinct() \
        .map(lambda a: (a[0], 1))
        
    aggregated_rdd = processed_rdd.reduceByKey(lambda a, b: a + b) # (id, count)

    # Process nodes to get (node_id, name) pairs
    processed_nodes_rdd = nodes_rdd.map(initial_nodes_processing_mapper_q3)

    # Join the two RDDs on the drug ID and transform to (name, count) pairs
    joined_rdd = aggregated_rdd \
        .join(processed_nodes_rdd) \
        .map(lambda x: (x[1][1], x[1][0])) # Transform from (id, (count, name)) to (name, count)

    sorted_rdd = joined_rdd.sortBy(lambda x: x[1], ascending=False)
    return sorted_rdd.take(5) # Top 5 (name, count)

import neo4j
import os
import py2neo
import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger('biodatagraph').setLevel(level=logging.DEBUG)

from biodatagraph.datasources import GeneOntology, Gtex, Reactome, NcbiTaxonomy
from biodatagraph.parser import GeneOntologyParser, GeneOntologyAssociationParser, GtexMetadataParser, GtexDataParser, \
    ReactomePathwayParser, ReactomeMappingParser

log = logging.getLogger(__name__)

ROOT_DIR = os.getenv('ROOT_DIR', '/download')
GC_NEO4J_URL = os.getenv('GC_NEO4J_URL', 'bolt://localhost:7687')
GC_NEO4J_USER = os.getenv('GC_NEO4J_USER', 'neo4j')
GC_NEO4J_PASSWORD = os.getenv('GC_NEO4J_PASSWORD', 'test')
RUN_MODE = os.getenv('RUN_MODE', 'prod')


def run_parser(parser):
    """
    Run a parser and log.

    :param parser: The parser
    :return: The parser after running
    """
    log.info("Run parser {}".format(parser.__class__.__name__))
    parser.run_with_mounted_arguments()
    log.info(parser.container.nodesets)
    log.info(parser.container.relationshipsets)
    return parser


def create_index(graph, parser_list):
    """
    Create all indexes for the RelationshipSets in a list of Parsers.

    :param graph: Py2neo graph instance
    :param parser_list: List of parsers
    """
    for parser in parser_list:
        for relationshipset in parser.container.relationshipsets:
            relationshipset.create_index(graph)


def create_nodesets(graph, parser_list):
    """
    Create the NodeSets for a list of parsers

    :graph: Py2neo graph instance
    :param parser_list: List of Parsers
    """
    for parser in parser_list:
        log.info("Create nodes for parser {}".format(parser.__class__.__name__))
        for nodeset in parser.container.nodesets:
            nodeset.create(graph)


def create_relationshipsets(graph, parser_list):
    """
    Create the RelationshipSets for a list of parsers

    :graph: Py2neo graph instance
    :param parser_list: List of Parsers
    """
    for parser in parser_list:
        log.info("Create relationships for parser {}".format(parser.__class__.__name__))
        for relset in parser.container.relationshipsets:
            relset.create(graph)


if __name__ == '__main__':

    if RUN_MODE.lower() == 'test':
        log.info("Run tests")

    else:
        graph = py2neo.Graph(GC_NEO4J_URL, user=GC_NEO4J_USER, password=GC_NEO4J_PASSWORD)

        # Download Datasources
        # ====================
        gtex = Gtex(ROOT_DIR)
        if not gtex.latest_local_instance():
            gtex.download()

        geneontology = GeneOntology(ROOT_DIR)
        if not geneontology.latest_local_instance():
            geneontology.download(taxids=['9606'])

        reactome = Reactome(ROOT_DIR)
        if not reactome.latest_local_instance():
            reactome.download()

        ncbi_taxonomy = NcbiTaxonomy(ROOT_DIR)
        if not ncbi_taxonomy.latest_local_instance():
            ncbi_taxonomy.download()


        # run Parsers
        # ================
        parsers_done = []

        go_parser = GeneOntologyParser(ROOT_DIR)
        parsers_done.append(run_parser(go_parser))

        go_annotation_parser = GeneOntologyAssociationParser(ROOT_DIR)
        go_annotation_parser.taxid = '9606'
        parsers_done.append(run_parser(go_annotation_parser))

        gtex_parser = GtexMetadataParser(ROOT_DIR)
        parsers_done.append(run_parser(gtex_parser))

        gtex_data_parser = GtexDataParser(ROOT_DIR)
        parsers_done.append(run_parser(gtex_data_parser))

        reactome_parser = ReactomePathwayParser(ROOT_DIR)
        parsers_done.append(run_parser(reactome_parser))

        reactome_mapping_parser = ReactomeMappingParser(ROOT_DIR)
        parsers_done.append(reactome_mapping_parser)

        # Load data
        # ================
        create_index(graph, parsers_done)
        create_nodesets(graph, parsers_done)
        create_relationshipsets(graph, parsers_done)

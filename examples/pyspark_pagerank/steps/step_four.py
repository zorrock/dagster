import re
from operator import add

from dagster import (
    Field,
    Dict,
    Int,
    DependencyDefinition,
    InputDefinition,
    OutputDefinition,
    Path,
    PipelineContextDefinition,
    PipelineDefinition,
    solid,
)
from dagster_pyspark import spark_session_resource, SparkRDD


def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


@solid(
    inputs=[InputDefinition('path', Path)],
    outputs=[OutputDefinition(SparkRDD)],
)
def load_pagerank_data(context, path):
    # two urls per line with space in between)
    lines = context.resources.spark.read.text(path).rdd.map(lambda r: r[0])

    # Loads all URLs from input file and initialize their neighbors.
    return lines.map(parseNeighbors)


@solid(
    inputs=[InputDefinition('links', SparkRDD)],
    config_field=Field(Dict({'iterations': Field(Int)})),
)
def execute_pagerank(context, links):
    cached_links = links.distinct().groupByKey().cache()

    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    ranks = cached_links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    iterations = context.solid_config['iterations']

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for _ in range(iterations):
        # Calculates URL contributions to the rank of other URLs.
        contribs = cached_links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(
                url_urls_rank[1][0], url_urls_rank[1][1]
            )
        )

        # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(
            lambda rank: rank * 0.85 + 0.15
        )

    # Collects all URL ranks and dump them to console.
    for (link, rank) in ranks.collect():
        context.log.info("%s has rank: %s." % (link, rank))


def define_pipeline():
    return PipelineDefinition(
        name='pyspark_pagerank',
        solids=[load_pagerank_data, execute_pagerank],
        dependencies={
            'execute_pagerank': {
                'links': DependencyDefinition('load_pagerank_data')
            }
        },
        context_definitions={
            'local': PipelineContextDefinition(
                resources={'spark': spark_session_resource}
            )
        },
    )

'''
Next step: Context and Resources

Here we encapsulate the SparkSession management into a resource

1) Add includes (PipelineContextDefinition and spark_session_resource)
2) Use context.resources.spark instead of spark in load_pagerank_data
3) Remove spark.stop()
4) Add to PipelineDefinition
        context_definitions={
            'local': PipelineContextDefinition(
                resources={'spark': spark_session_resource}
            )
        },
'''
import re
from operator import add

from dagster import (
    DependencyDefinition,
    Dict,
    Field,
    InputDefinition,
    Int,
    Path,
    PipelineDefinition,
    PipelineContextDefinition,
    solid,
)

from dagster_pyspark import spark_session_resource


def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


@solid(inputs=[InputDefinition('path', Path)])
def load_pagerank_data_step_four(context, path):
    # Initialize the spark context.
    # two urls per line with space in between)
    lines = context.resources.spark.read.text(path).rdd.map(lambda r: r[0])

    # Loads all URLs from input file and initialize their neighbors.
    return lines.map(parseNeighbors)


@solid(
    inputs=[InputDefinition('urls')],
    config_field=Field(Dict({'iterations': Field(Int)})),
)
def execute_pagerank_step_four(context, urls):
    # Loads all URLs from input file and initialize their neighbors.
    links = urls.distinct().groupByKey().cache()

    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    iterations = context.solid_config['iterations']

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for _ in range(iterations):
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(
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


def define_pyspark_pagerank_step_four():
    return PipelineDefinition(
        name='pyspark_pagerank_step_four',
        context_definitions={
            'local': PipelineContextDefinition(
                resources={'spark': spark_session_resource}
            )
        },
        dependencies={
            'execute_pagerank_step_four': {
                'urls': DependencyDefinition('load_pagerank_data_step_four')
            }
        },
        solids=[load_pagerank_data_step_four, execute_pagerank_step_four],
    )


if __name__ == '__main__':
    from dagster import execute_pipeline

    execute_pipeline(
        define_pyspark_pagerank_step_four(),
        environment_dict={
            'context': {
                'local': {
                    'resources': {
                        'spark': {
                            'config': {
                                'spark_conf': {
                                    'spark': {'app': {'name': 'foobar'}}
                                }
                            }
                        }
                    }
                }
            },
            'solids': {
                'load_pagerank_data_step_four': {
                    'inputs': {'path': '../pagerank_data.txt'}
                },
                'execute_pagerank_step_four': {'config': {'iterations': 2}},
            },
        },
    )

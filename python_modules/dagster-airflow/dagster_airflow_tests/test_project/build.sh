#! /bin/bash
# For the avoidance of doubt, this script is meant to be run with the
# test_project directory as pwd.
# The filesystem manipulation below is to support installing local development
# versions of dagster-graphql and dagster.

cp -R ../../../dagster . && \
cp -R ../../../dagster-graphql . && \
cp -R ../../../libraries/dagster-aws . && \
rm -rf \
  dagster/.tox \
  dagster/dist \
  dagster/build \
  dagster/*.egg-info \
  dagster-aws/.tox \
  dagster-aws/dist \
  dagster-aws/build \
  dagster-aws/*.egg-info \
  dagster-graphql/.tox \
  dagster-graphql/dist \
  dagster-graphql/*.egg-info \
  dagster-graphql/build && \
\
docker build -t dagster-airflow-demo . && \
rm -rf dagster dagster-graphql dagster-aws

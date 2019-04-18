from enum import Enum

import yaml

DOCKER_PLUGIN = "docker#v3.1.0"


class SupportedPython(Enum):
    V3_7 = "3.7"
    V3_6 = "3.6"
    V3_5 = "3.5"
    V2_7 = "2.7"


IMAGE_MAP = {
    SupportedPython.V3_7: "python:3.7",
    SupportedPython.V3_6: "python:3.6",
    SupportedPython.V3_5: "python:3.5",
    SupportedPython.V2_7: "python:2.7",
}

TOX_MAP = {
    SupportedPython.V3_7: "py37",
    SupportedPython.V3_6: "py36",
    SupportedPython.V3_5: "py35",
    SupportedPython.V2_7: "py27",
}


class StepBuilder:
    def __init__(self, label):
        self._step = {"label": label}

    def run(self, *argc):
        self._step["command"] = "\n".join(argc)
        return self

    def onImage(self, img):
        self._step["plugins"] = [{DOCKER_PLUGIN: {"image": IMAGE_MAP[img]}}]
        return self

    def build(self):
        return self._step


def python_modules_tox_tests(directory, prereqs=None):
    tests = []
    for version in SupportedPython:
        tox_command = []
        if prereqs:
            tox_command += prereqs
        tox_command += [
            "pip install tox;",
            "cd python_modules/{}".format(directory),
            "tox -e {}".format(TOX_MAP[version]),
        ]
        tests.append(
            StepBuilder("{} tests ({})".format(directory, version.value))
            .run(*tox_command)
            .onImage(version)
            .build()
        )

    return tests


if __name__ == "__main__":
    steps = []
    steps += python_modules_tox_tests("dagster")
    steps += python_modules_tox_tests("dagit", ["apt-get update", "apt-get install -y xdg-utils"])
    steps += python_modules_tox_tests("dagster-graphql")
    steps += python_modules_tox_tests("libraries/dagster-pandas")
    steps += python_modules_tox_tests("libraries/dagster-ge")
    steps += python_modules_tox_tests("libraries/dagster-aws")
    steps += python_modules_tox_tests("libraries/dagster-snowflake")
    steps += python_modules_tox_tests("libraries/dagster-spark")
    steps += [
        StepBuilder("Lint").run("make dev_install", "make pylint").onImage(SupportedPython.V3_7).build()
    ]
    print(yaml.dump({"steps": steps}, default_flow_style=False, default_style="|"))

from dataclasses import dataclass
from typing import List

from ruamel.yaml import yaml_object

from aclimatise_automation.yml import yaml


@yaml_object(yaml)
@dataclass
class BaseCampMeta:
    aclimatise_version: str
    packages: List[str]

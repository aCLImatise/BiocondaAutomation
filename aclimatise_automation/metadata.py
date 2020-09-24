from dataclasses import dataclass
from typing import List

from aclimatise_automation.yml import yaml
from ruamel.yaml import yaml_object


@yaml_object(yaml)
@dataclass
class BaseCampMeta:
    aclimatise_version: str
    packages: List[str]

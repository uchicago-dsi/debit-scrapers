"""Defines concrete workflows for extracting development bank project data."""

from .adb import (
    AdbProjectScrapeWorkflow,
    AdbResultsScrapeWorkflow,
    AdbSeedUrlsWorkflow,
)
from .afdb import (
    AfdbProjectPartialDownloadWorkflow,
    AfdbProjectPartialScrapeWorkflow,
)
from .aiib import AiibProjectScrapeWorkflow, AiibSeedUrlsWorkflow
from .bio import (
    BioProjectPartialScrapeWorkflow,
    BioResultsMultiScrapeWorkflow,
    BioSeedUrlsWorkflow,
)
from .deg import DegDownloadWorkflow
from .dfc import DfcDownloadWorkflow
from .ebrd import (
    EbrdProjectPartialDownloadWorkflow,
    EbrdProjectPartialScrapeWorkflow,
)
from .eib import (
    EibProjectPartialScrapeWorkflow,
    EibResultsMultiScrapeWorkflow,
    EibSeedUrlsWorkflow,
)
from .fmo import (
    FmoProjectScrapeWorkflow,
    FmoResultsScrapeWorkflow,
    FmoSeedUrlsWorkflow,
)
from .idb import IdbProjectDownloadWorkflow
from .ifc import IfcProjectScrapeWorkflow, IfcSeedUrlsWorkflow
from .kfw import KfwDownloadWorkflow
from .miga import (
    MigaProjectScrapeWorkflow,
    MigaResultsScrapeWorkflow,
    MigaSeedUrlsWorkflow,
)
from .nbim import NbimProjectScrapeWorkflow, NbimSeedUrlsWorkflow
from .pro import (
    ProProjectScrapeWorkflow,
    ProResultsScrapeWorkflow,
    ProSeedUrlsWorkflow,
)
from .undp import (
    UndpProjectPartialDownloadWorkflow,
    UndpProjectPartialScrapeWorkflow,
)
from .wb import WbDownloadWorkflow

__all__ = [
    "AdbProjectScrapeWorkflow",
    "AdbResultsScrapeWorkflow",
    "AdbSeedUrlsWorkflow",
    "AfdbProjectPartialDownloadWorkflow",
    "AfdbProjectPartialScrapeWorkflow",
    "AiibProjectScrapeWorkflow",
    "AiibSeedUrlsWorkflow",
    "BioProjectPartialScrapeWorkflow",
    "BioResultsMultiScrapeWorkflow",
    "BioSeedUrlsWorkflow",
    "DegDownloadWorkflow",
    "DfcDownloadWorkflow",
    "EbrdProjectPartialDownloadWorkflow",
    "EbrdProjectPartialScrapeWorkflow",
    "EibProjectPartialScrapeWorkflow",
    "EibResultsMultiScrapeWorkflow",
    "EibSeedUrlsWorkflow",
    "FmoProjectScrapeWorkflow",
    "FmoResultsScrapeWorkflow",
    "FmoSeedUrlsWorkflow",
    "IdbProjectDownloadWorkflow",
    "IfcProjectScrapeWorkflow",
    "IfcSeedUrlsWorkflow",
    "KfwDownloadWorkflow",
    "MigaProjectScrapeWorkflow",
    "MigaResultsScrapeWorkflow",
    "MigaSeedUrlsWorkflow",
    "NbimProjectScrapeWorkflow",
    "NbimSeedUrlsWorkflow",
    "ProProjectScrapeWorkflow",
    "ProResultsScrapeWorkflow",
    "ProSeedUrlsWorkflow",
    "UndpProjectPartialDownloadWorkflow",
    "UndpProjectPartialScrapeWorkflow",
    "WbDownloadWorkflow",
]

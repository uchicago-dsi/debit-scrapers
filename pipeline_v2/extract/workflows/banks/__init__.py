from .adb import AdbProjectScrapeWorkflow, AdbResultsScrapeWorkflow, AdbSeedUrlsWorkflow
from .afdb import AfdbProjectPartialDownloadWorkflow, AfdbProjectPartialScrapeWorkflow
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
from .fmo import FmoProjectScrapeWorkflow, FmoResultsScrapeWorkflow, FmoSeedUrlsWorkflow
from .idb import IdbProjectDownloadWorkflow
from .ifc import IfcProjectScrapeWorkflow, IfcSeedUrlsWorkflow
from .kfw import KfwDownloadWorkflow
from .miga import (
    MigaProjectScrapeWorkflow,
    MigaResultsScrapeWorkflow,
    MigaSeedUrlsWorkflow,
)
from .nbim import NbimDownloadWorkflow
from .pro import ProProjectScrapeWorkflow, ProResultsScrapeWorkflow, ProSeedUrlsWorkflow
from .undp import (
    UndpProjectPartialScrapeWorkflow,
    UndpResultsMultiScrapeWorkflow,
    UndpSeedUrlsWorkflow,
)
from .wb import WbDownloadWorkflow

from pyDataHub import DataHub
from .datavault import BrainDateDataVault
from pyDataHubImp.processors.loadProcessors import DHF_0003_Extract_ODS_Supplier
from pyDataHubImp.processors.deliveryProcessors import  DHF_0004_Deliver_EASI_Account
from pyDataHubImp.processors.businessProcessors import BR_0006_Organization_BV_Enrichment_with_accountKey

class BrainDateDataHub(DataHub):

    def __init__(self, path, dbutils, initWithCreation=False):
        super().__init__(path, dbutils, initWithCreation)

        self.dataVault = BrainDateDataVault(self.dataVaultPath, self.dbutils, "BrainDateDataHub", initWithCreation)

        self.registerFactsProcessors()
        self.registerDeliveryProcessors()
        self.registerBusinessProcessors()

    def registerFactsProcessors(self):
        self.factsProcessors["DHF_0003_Extract_ODS_Supplier"] = DHF_0003_Extract_ODS_Supplier(self, "Salesforce")   
            
    def registerDeliveryProcessors(self):
        self.deliveryProcessors["DHF_0004_Deliver_EASI_Account"] = DHF_0004_Deliver_EASI_Account(self, "EASI")   

    def registerBusinessProcessors(self):
        self.businessProcessors["BR_0006_Organization_BV_Enrichment_with_accountKey"] = BR_0006_Organization_BV_Enrichment_with_accountKey(self, "BR_0006_V1")
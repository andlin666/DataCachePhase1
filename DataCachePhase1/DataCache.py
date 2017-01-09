# a-b-c-d-e-f-g
# i have gummy bears chasing me
# one is red, one is blue
# one is chewing on my shoe
# now i am running for my life
# because the red one has a knife

import sys
import os
import traceback

from numpy.random import randint

from azure.storage.blob import BlockBlobService
from DataConnection import DataConnection
from ContainerView import ContainerView

# fluff
TEXTBOLD = '\033[1m'
TEXTFAIL = '\033[91m'
TEXTEND = '\033[0m'

# list of supported file extensions
IMAGEEXTENSION = [ ".jpg", ".jpeg", ".png" ]
DELIMITEDEXTENSION = [ ".csv", ".tsv" ]

class DataCache(object):
    """Base class for Azure blob data management. DataCache though represent an abtraction for data. 
    
    This can include:
        (1) Local files, e.g. CVS or serialized objects
        (2) Cloud based files in Azure Object Store (a.k.a. Azure Blobs)
        (3) Azure SQL
        (4) Pendleton artifacts
        (5) Encapsualtion of other services, e.g. ADF

    """

    #TODO: some more TODO's that I will get to on Tuesday ... the rest are scattered through the code
    #TODO: write proper help comments so that print(obj.__doc__) works!    
    #TODO: need a method for refreshing the cache
    #TODO: need a method for writing to blob
    #TODO: need a method for creating containers
    #TODO: need simple methods for deleting containers

    _dataConnection = None
    _blobService = None

    # target location
    _islocalpath = True
    _path = None

    # cached values
    _containers = {}

    # start the properties section
    @property
    def Path(self):
        return self._path

    @Path.setter
    def Path(self, path):
        self._path = path
    
    # DESIGN: the main point is that DataCaches should not be allowed to be 
    # DESIGN: intantiated if the DataConnection is invalid!
    def __init__(self, dataconnection = DataConnection("","")):
        self._dataConnection = dataconnection

        try:
            self._blobService = BlockBlobService(self._dataConnection._accountName, self._dataConnection._accountKey)
            self._containers = DataCache._buildContainerViews(self._blobService)
    
        except Exception as e:
            print(TEXTBOLD + TEXTFAIL + "Unable to create Blob Service due to invalid DataConnection. ---->" + TEXTEND)
            print(TEXTBOLD + TEXTFAIL + "\t%s" % e + TEXTEND)
            print()
            raise e

    #TODO: sketch out the case for Azure Object Store, then see how or if 
    #TODO: any of it actually generalizes! I just want to learn the SDK and 
    #TODO: get started with the Kaggle in full. :)

    # print connection information
    def ConnectionInfo(self):
        print("-----------------------------------------------------")
        print("Connection Info: ")
        self._dataConnection.ConnectionInfo()
        print("-----------------------------------------------------")

        print("-----------------------------------------------------")
        print("Blob Service Info: ")
        print("Account Name: " + self._blobService.account_name)
        print("Blob Type: " + self._blobService.blob_type)
        print("-----------------------------------------------------")

    #TODO: need to understand and work out what sort of logging, analytics, 
    #TODO: and additional information the class will support.

    # display containers in the storage account
    def DisplayContainers(self):
        for container in self._containers:
            self._containers[container].DisplayContent()

    # return a list of the containers in the account
    def GetContainerNames(self, sort = True):
        containers = list(self._containers.keys())
        if (sort):
            containers = sorted(containers)
        return containers
    
    # for a given container return a list of the blobs
    def GetBlobNames(self, containerName, sort = True):
        containerView = self._containers[containerName]
        blobList = containerView.BlobList
        if (sort):
            blobList = sorted(blobList)
        return blobList

    # refresh the container view
    def RefreshContainerViews(self):
        self._containers = DataCache._buildContainerViews(self._blobService)

    # refresh a given container view
    def RefreshContainerView(self, containerName):
        containerView = DataCache._buildContainerView(self._blobService, containerName)

    # copy files to the Path
    def CopyBlobsToPath(self, containerName, overWrite = True):
        #DESIGN: for the first iteration of this just assume that the path is local, i.e. 
        #DESIGN: a potential folder on the users machine. this can be generalized later.
        #DESIGN: also all the container contents are going to be downloaded.
        
        if not(self._validatePath()):
            return
        
        # update the path to mirror the container layout
        path = self._path + "\\" + containerName + "\\"

        # local location
        if (self._islocalpath):
            self._createLocalPath(path)

            #TODO: update the class to have some related exceptions. this validation should
            #TODO: be in the same place where we get the list of blobs in a container!
            if not(self._validateContainerName(containerName)):
                return

            print("Copying contents of container '%s'" % containerName)
            blobList = self.GetBlobNames(containerName)
            self._copyBlobs(containerName, blobList, path, overWrite)

    # copy a random sample of blobs of count N to the Path. if N > number of blobs in the 
    # container, then all blobs will be copied
    def CopyRandomSampleBlobsToPath(self, containerName, N):
        #DESIGN: for the first iteration of this just assume that the path is local, i.e. 
        #DESIGN: a potential folder on the users machine. this can be generalized later.
        #DESIGN: also all the container contents are going to be downloaded.
        
        if not(self._validatePath()):
            return
        
        # update the path to mirror the container layout
        path = self._path + "\\" + containerName + "\\"

        # local location
        if (self._islocalpath):
            self._createLocalPath(path)

            #TODO: update the class to have some related exceptions. this validation should
            #TODO: be in the same place where we get the list of blobs in a container!
            if not(self._validateContainerName(containerName)):
                return
            
            # uniformly distributed random sampling of blobs
            blobList = self.GetBlobNames(containerName, False)
            indicesToSample = randint(0,len(blobList),N)
            blobsToSample = []
            for index in indicesToSample:
                blobsToSample.append(blobList[index])

            #TODO: need to clear out the specified target location. since we are sampling and asking 
            #TODO: pull down a new sample the old one should not be augmented. this is an important point!
            #DESIGN: will need to work out capturing of the blobs that have been used.
            print("Copying random sample of contents from container '%s'." % containerName)
            self._copyBlobs(containerName, blobsToSample, path, True)

    # clear the files specified by the path location
    def ClearBlobsFromPath(self, containerName):
        #DESIGN: this will deleted all files which essentially is a hard cache flush

        if not(self._validatePath()):
            return

        #TODO: see comment above about local path case!
        if (self._islocalpath):
            path = self._path + "\\" + containerName + "\\"
            print("Clearing files from '%s' correspoding to container '%s'." % (path, containerName))
            self._clearLocalPath(path)
    
    # ---------------------------------------------------------------------------
    # begin private helper methods

    # build a container view
    @staticmethod
    def _buildContainerView(blobService, containerName):
        
        blobList = []
        blobGenerator = blobService.list_blobs(containerName)
        for blob in blobGenerator:
            blobList.append(blob.name)

        return ContainerView(containerName, blobList)

    # build the container views
    @staticmethod
    def _buildContainerViews(blobService):
        containers = {}
        containerGenerator = blobService.list_containers().items
        for container in containerGenerator:
            containerName = container.name
            
            # containers[containerName] = DataCache._buildContainerView(blobService, containerName)
            blobList = []
            blobGenerator = blobService.list_blobs(containerName).items
            for blob in blobGenerator:
                blobList.append(blob.name)
            
            containers[containerName] = ContainerView(containerName, blobList)

        return containers

    # validate the path
    def _validatePath(self):
        # if path is not set, then error
        if self._path is None:
            #TODO: udpate with proper user exceptions and logging information
            print(TEXTBOLD + TEXTFAIL + "There is no specified path information. Please update 'Path' to specify a location." + TEXTEND)
            return False
        
        return True

    # validate specified container is present
    def _validateContainerName(self, containerName):
        if not(containerName in self._containers):
            #TODO: udpate with proper user exceptions and logging information
            print(TEXTBOLD + TEXTFAIL + "Specified container '%s' does not exist in the account." %containerName + TEXTEND)
            return False

        return True

    # create target path if needed
    def _createLocalPath(self, path):
        if not(os.path.exists(path)):
            os.makedirs(path)

    # delete objects in the target path and then remove the target path
    def _clearLocalPath(self, path):
        if not(os.path.exists(path)):
            print("The specified cache '%s' does not exist." % path)
            return

        files = os.listdir(path)
        for file in files:
            filepath = path + file
            print("Removing file '%s'" % filepath)
            os.remove(filepath)

        os.removedirs(path)

    # copy a list of blobs
    def _copyBlobs(self, containerName, blobList, path, overWrite = True):
        #TODO: acquire container lease, but not sure if i need to do this???
        containerLeaseId = self._blobService.acquire_container_lease(containerName)
            
        for blobName in blobList:
            print("Copying '%s' to '%s'" % (blobName, path))
            targetfile = path + blobName
                               
            if overWrite or not(os.path.exists(targetfile)):
                self._copyBlob(containerName, blobName, targetfile)
        
        self._blobService.release_container_lease(containerName, containerLeaseId)

    # grab a lease on a blob, copy its contents, and release the lease
    def _copyBlob(self, containerName, blobName, targetfile):
        #TODO: look into this lease nonsense
        #TODO: blobLeaseId = self._blobService.acquire_blob_lease(containerName,blobName)
        #TODO: self._blobService.get_blob_to_path(containerName, blobName, targetfile, blobLeaseId)
        #TODO: self._blobService.release_blob_lease(containerName, blobName, blobLeaseId)
        return self._blobService.get_blob_to_path(containerName, blobName, targetfile)
    
# end class DataCache

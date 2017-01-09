# a-b-c-d-e-f-g
# i have gummy bears chasing me
# one is red, one is blue
# one is chewing on my shoe
# now i am running for my life
# because the red one has a knife

import sys
import json

import Secrets

class DataConnection(object):
    """Class that encapsulates account information and credentials for Azure Storage."""

    # static members.
    accountName = "Account Name"
    accountKey = "Account Key"
    accountKind = "Account Kind"

    notYetImplementedMsg = "Only Azure Storage accounts are currently supported."
    azureAccount = "azure"

    _accountName = None
    _accountKey = None
    _accountKind = None

    def __init__(self, accountname, accountkey, kind=DataConnection.azureAccount):

        if DataConnection.azureAccount != kind:
            raise NotImplementedError(DataConnection.notYetImplementedMsg)

        # TODO: expand and update kind information
        self._accountName = accountname
        self._accountKey = accountkey
        self._accountKind = kind

    def ConnectionInfo(self):
        """Display account name and account kind."""
        if (self._accountKind == "azure"):
            print("%s: %s" % (DataConnection.accountName, self._accountName))
            print("%s: %s" % (DataConnection.accountKind, self._accountKind))
        else:
            raise NotImplementedError(DataConnection.notYetImplementedMsg)

    def ExportToJson(self, filepath):
        """Serialize this instance to JSON."""
        accountinfo = json.dumps({DataConnection.accountName : self._accountName , 
                                  DataConnection.accountKey : self._accountKey, 
                                  DataConnection.accountKind : self._accountKind})
        encryptedinfo = Secrets._encryptContents(accountinfo)
        filehandle = open(filepath, 'wb')
        filehandle.write(encryptedinfo)
        print("Account info has been stored to '%s'" % filepath)
        return True

    @staticmethod
    def ImportFromJson(filepath):
        """Deserialize an instance from JSON."""
        filecontent = open(filepath, 'rb').read()
        encryptedinfo = json.loads(filecontent)
        accountinfo = Secrets._decryptContents(encryptedinfo)
        return DataConnection(accountinfo.get(DataConnection.accountName), 
                              accountinfo.get(DataConnection.accountKey), 
                              accountinfo.get(DataConnection.accountKind))

# end class DataConnection

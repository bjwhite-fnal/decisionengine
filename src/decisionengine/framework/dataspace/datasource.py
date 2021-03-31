import abc
import logging


class DataSource(object, metaclass=abc.ABCMeta):

    #: Name of the channel_manager table
    channel_manager_table = 'channel_manager'

    #: Name of the dataproduct table
    dataproduct_table = 'dataproduct'

    #: Name of the header table
    header_table = 'header'

    #: Name of the metadata table
    metadata_table = 'metadata'

    def __init__(self, config):
        """
        :type config: :obj:`dict`
        :arg config: Configuration dictionary
        """

        self.config = config
        self.logger = logging.getLogger()
        self.logger.debug('Initializing a datasource')

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return '%s' % vars(self)

    @abc.abstractmethod
    def get_schema(self, table=None):
        """
        Given the table name return it's schema

        :type table: :obj:`string`
        :arg table: Name of the table
        """
        self.logger.info('getting the datasource schema')

        schemas = {
            'channel_manager': [
                'sequence_id INT',
                'channel_manager_id TEXT',
                'name TEXT',
                'datestamp timestamp with timezone',
            ],
            'header': [
                'channel_manager_id INT',
                'generation_id INT',
                'key TEXT',
                'create_time REAL',
                'expiration_time REAL',
                'scheduled_create_time REAL',
                'creator TEXT',
                'schema_id INT',
            ],
            'schema': [
                'schema_id INT',  # Auto generated
                'schema BLOB',   # keys in the value dict of the dataproduct table
            ],
            'metadata': [
                'channel_manager_id INT',
                'generation_id INT',
                'key TEXT',
                'state TEXT',
                'generation_time REAL',
                'missed_update_count INT',
            ],
            'dataproduct': [
                'channel_manager_id INT',
                'generation_id INT',
                'key TEXT',
                'value BLOB'
            ]
        }

        if table:
            return {table: schemas.get(table)}
        return schemas

    @abc.abstractmethod
    def connect(self):
        """
        Create a pool of database connections
        """
        self.logger.info('datasource is creating the database connections')
        return

    @abc.abstractmethod
    def create_tables(self):
        """
        Create database tables
        """
        self.logger.info('datasource is creating the database tables')
        return

    @abc.abstractmethod
    def insert(self, channel_manager_id, generation_id, key,
               value, header, metadata):
        """
        Insert data into respective tables for the given
        channel_manager_id, generation_id, key

        :type channel_manager_id: :obj:`string`
        :arg channel_manager_id: channel_manager_id for generation to be retrieved
        :type generation_id: :obj:`int`
        :arg generation_id: generation_id of the data
        :type key: :obj:`string`
        :arg key: key for the value
        :type value: :obj:`object`
        :arg value: Value can be an object or dict
        :type header: :obj:`~datablock.Header`
        :arg header: Header for the value
        :type metadata: :obj:`~datablock.Metadata`
        :arg header: Metadata for the value
        """
        self.logger.info('datasource is inserting data into the database tables')
        return

    @abc.abstractmethod
    def update(self, channel_manager_id, generation_id, key,
               value, header, metadata):
        """
        Update the data in respective tables for the given
        channel_manager_id, generation_id, key

        :type channel_manager_id: :obj:`string`
        :arg channel_manager_id: channel_manager_id for generation to be retrieved
        :type generation_id: :obj:`int`
        :arg generation_id: generation_id of the data
        :type key: :obj:`string`
        :arg key: key for the value
        :type value: :obj:`object`
        :arg value: Value can be an object or dict
        :type header: :obj:`~datablock.Header`
        :arg header: Header for the value
        :type metadata: :obj:`~datablock.Metadata`
        :arg header: Metadata for the value
        """
        self.logger.info('datasource is updating data in the database tables')
        return

    @abc.abstractmethod
    def get_dataproduct(self, channel_manager_id, generation_id, key):
        """
        Return the data from the dataproduct table for the given
        channel_manager_id, generation_id, key

        :type channel_manager_id: :obj:`string`
        :arg channel_manager_id: channel_manager_id for generation to be retrieved
        :type generation_id: :obj:`int`
        :arg generation_id: generation_id of the data
        :type key: :obj:`string`
        :arg key: key for the value
        """
        self.logger.info('datasource is getting a dataproduct for a channel_manager')
        return

    @abc.abstractmethod
    def get_dataproducts(self, channel_manager_id):
        """
        Return list of all data products associated with
        with channel_manager_id

        :type channel_manager_id: :obj:`string`
        """
        self.logger.info('datasource is getting all dataproducts for a channel_manager')
        return

    @abc.abstractmethod
    def get_header(self, channel_manager_id, generation_id, key):
        """
        Return the header from the header table for the given
        channel_manager_id, generation_id, key

        :type channel_manager_id: :obj:`string`
        :arg channel_manager_id: channel_manager_id for generation to be retrieved
        :type generation_id: :obj:`int`
        :arg generation_id: generation_id of the data
        :type key: :obj:`string`
        :arg key: key for the value
        """
        self.logger.info('datasource is getting the header for a channel_manager')
        return

    @abc.abstractmethod
    def get_metadata(self, channel_manager_id, generation_id, key):
        """
        Return the metadata from the metadata table for the given
        channel_manager_id, generation_id, key

        :type channel_manager_id: :obj:`string`
        :arg channel_manager_id: channel_manager_id for generation to be retrieved
        :type generation_id: :obj:`int`
        :arg generation_id: generation_id of the data
        :type key: :obj:`string`
        :arg key: key for the value
        """
        self.logger.info('datasource is getting the metadata for a channel_manager')
        return

    @abc.abstractmethod
    def get_datablock(self, channel_manager_id, generation_id):
        """
        Return the entire datablock from the dataproduct table for the given
        channel_manager_id, generation_id

        :type channel_manager_id: :obj:`string`
        :arg channel_manager_id: channel_manager_id for generation to be retrieved
        :type generation_id: :obj:`int`
        :arg generation_id: generation_id of the data
        """
        self.logger.info('datasource is getting the datablock for a channel_manager')
        return

    @abc.abstractmethod
    def duplicate_datablock(self, channel_manager_id, generation_id,
                            new_generation_id):
        """
        For the given channel_manager_id, make a copy of the datablock with given
        generation_id, set the generation_id for the datablock copy

        :type channel_manager_id: :obj:`string`
        :arg channel_manager_id: channel_manager_id for generation to be retrieved
        :type generation_id: :obj:`int`
        :arg generation_id: generation_id of the data
        :type new_generation_id: :obj:`int`
        :arg new_generation_id: generation_id of the new datablock created
        """
        self.logger.info('datasource is duplicating a datablock for a channel_manager')
        return

    @abc.abstractmethod
    def get_last_generation_id(self, name, channel_manager_id=None):
        """
        Return last generation id for current channel manager
        or channel_manager w/ channel_manager_id.

        :type name: :obj:`string`
        :arg name: channel manager name
        :type channel_manager_id: :obj:`string`
        :arg channel_manager_id: channel manager id
        """
        self.logger.info('datasource is getting the last generation id for a channel_manager')
        return

    @abc.abstractmethod
    def close(self):
        """
        Close all connections to the database
        """
        self.logger.info('datasource is closing database connections')
        return

    @abc.abstractmethod
    def store_channel_manager(self, channel_manager_name, channel_manager_id):
        """
        Store ChannelManager
        :type channel_manager_name: :obj:`string`
        :arg channel_manager_name: name of channel_manager to retrieve
        :type channel_manager_id: :obj:`string`
        :arg channel_manager_id: id of channel_manager to retrieve
        """
        self.logger.info('datasource is storing a channel_manager')
        return

    @abc.abstractmethod
    def get_channel_managers(self, channel_manager_name=None, start_time=None, end_time=None):
        """
        Retrieve ChannelManagers
        :type channel_manager_name: :obj:`string`
        :arg channel_manager_name: name of channel_manager to retrieve
        :type channel_manager_id: :obj:`string`
        :arg channel_manager_id: id of channel_manager to retrieve
        """
        self.logger.info('datasource is getting all channel_managers')
        return

    @abc.abstractmethod
    def get_channel_manager(self, channel_manager_name, channel_manager_id):
        """
        Retrieve ChannelManager
        :type channel_manager_name: :obj:`string`
        :arg channel_manager_name: name of channel_manager to retrieve
        :type channel_manager_id: :obj:`string`
        :arg channel_manager_id: id of channel_manager to retrieve
        """
        self.logger.info('datasource is getting a channel_manager')
        return

    @abc.abstractmethod
    def delete_data_older_than(self, days):
        """
        Delete data older that interval
        :type days: :obj:`long`
        :arg days: remove data older than interval
        """
        self.logger.info('datasource is deleting data')
        return

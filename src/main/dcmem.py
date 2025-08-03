"""
Salesforce Data Cloud based Memory Vector Store Implementation for Mem0

All writes happen over Data Cloud's ingestion api and all reads happen over Data Cloud's Query Service Api
"""

from dotenv import load_dotenv
from datetime import datetime
from typing import List, Dict, Optional
import logging
from pydantic import BaseModel
from mem0.vector_stores.base import VectorStoreBase
from src.datacloud.readers.query_svc import QueryServiceClient
from src.datacloud.connectors.ingestion_api.ingestion_client import DataCloudIngestionClient

logger = logging.getLogger(__name__)

load_dotenv()

class OutputData(BaseModel):
    id: Optional[str] # memory_id
    score: Optional[float] # distance
    payload: Optional[Dict] # metadata

class DataCloudMemoryStore(VectorStoreBase):
    """
    Data Cloud based Memory implementation for Mem0
    """
    def __init__(self,
                 connector_name: str = "mem0", # ingestion api connector name in Data Cloud
                 dlo: str = "AgentMemory",
                 vector_index_dlm: str = "AgentMemory_index_dlm",
                 chunk_dlm: str = "AgentMemory_chunk_dlm",
                 collection_name: Optional[str] = None,
                 ):
        """
        Initialize DC memory
        :param connector_name:
        :param dlo:
        :param vector_index_dlm:
        :param chunk_dlm:
        :param collection_name:
        """
        self.connector_name = connector_name
        self.dlo = dlo,
        self.vector_index_dlm = vector_index_dlm
        self.chunk_dlm = chunk_dlm
        self._setup_datacloud()

    def _setup_datacloud(self):
        self.query_svc_client = QueryServiceClient()
        self.ingestion_client = DataCloudIngestionClient()

    def create_col(self, name: str, vector_size: int, distance: str) -> None:
        """
        NoOp: the setup assumes the collection, that is, the DLO, the Chunk DLM and the Vector Idx DLM are already setup in Data Cloud
        """
        pass

    def insert(self, vectors: List[List[float]], payloads: Optional[List[Dict]] = None,
               ids: Optional[List[str]] = None) -> List[str]:
        """
        Insert only textual data from payloads into Data cloud because DC takes care of embedding in real-time.

        :param vectors:
        :param payloads:
        :param ids:
        :return: List of inserted vector Ids
        """
        if not payloads:
            logger.warning("Empty payload, nothing will be inserted")
            return []

        combines_items = [{'text': data_obj['data'], 'id': id_val} for data_obj, id_val in zip(payloads, ids)]
        data = []
        for item in combines_items:
            data.append({
                "id": item['id'],
                "memory": item['text'],
                "createdAt": datetime.now().strftime("%Y-%m%-d %H:%M:%S")
            })
        request_object = {"data": data}
        self.ingestion_client.ingest_data(request_object, self.connector_name, self.dlo)
        return ids

    def search(self, query: str, vectors: List[List[float]], limit: int = 1,
               filters: Optional[Dict] = None) -> List[OutputData]:
        """
        search similar vectors
        :param query:
        :param vectors:
        :param limit:
        :param filters:
        :return:
        """
        sql = f"""
        SELECT
            index.RecordId__c,
            index.score__c,
            chunk.Chunk__c
        FROM
            vector_search(TABLE({self.vector_index_dlm}), '{query}', '', {limit}) AS index
        JOIN
            {self.chunk_dlm} AS chunk
        ON
            index.RecordId__c = chunk.RecordId__c        
        """
        request_obj = {
            "sql": sql
        }
        logger.debug(request_obj)
        response = self.query_svc_client.read_data(request_obj)
        output = []
        for item in response:
            if item.payload is not 'null':
                output.append(OutputData(
                    id=item.id,
                    score=item.score,
                    payload={
                        'data': item.payload
                    }
                ))
        return output

    def list_cols(self) -> List[str]:
        """
        :return: list all collection  names
        """
        return [self.dlo]

    def delete(self, vector_id: str) -> bool:
        """
        NoOp
        """
        pass

    def delete_col(self) -> bool:
        """
        NoOp
        """
        pass

    def col_info(self) -> Optional[Dict]:
        """
        NoOp
        """
        pass

    def get(self, vector_id: str) -> bool:
        """
        NoOp
        """
        pass

    def list(self, filters: Dict = None, limit: int = None) -> bool:
        """
        NoOp
        """
        pass

    def reset(self) -> bool:
        """
        NoOp
        """
        pass

    def update(self, vector_id: str, vector: Optional[List[Dict]] = None, payload: Optional[Dict] = None) -> bool:
        """
        NoOp
        """
        pass
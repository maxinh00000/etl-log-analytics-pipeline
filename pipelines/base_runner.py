from abc import ABC, abstractmethod

class BasePipelineRunner(ABC):

    @abstractmethod
    def run(self, records, query, run_id, batch_id):
        pass
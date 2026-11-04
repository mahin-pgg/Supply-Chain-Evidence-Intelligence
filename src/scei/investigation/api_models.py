from uuid import UUID

from pydantic import BaseModel, Field


class ContaminationInvestigationRequest(BaseModel):
    organization_id: UUID
    contaminated_node_id: UUID
    context_id: UUID | None = None
    max_hops: int = Field(default=20, ge=1, le=100)
    include_possible: bool = False


class GenealogyQuery(BaseModel):
    organization_id: UUID
    node_id: UUID
    context_id: UUID | None = None
    direction: str = "downstream"
    max_hops: int = Field(default=20, ge=1, le=100)
    include_possible: bool = False


class BatchInvestigationRequest(BaseModel):
    organization_id: UUID
    batch_query: str = Field(min_length=1, max_length=256)
    context_id: UUID | None = None
    max_hops: int = Field(default=20, ge=1, le=100)
    include_possible: bool = False

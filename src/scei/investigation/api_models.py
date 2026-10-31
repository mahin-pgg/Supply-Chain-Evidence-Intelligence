from uuid import UUID

from pydantic import BaseModel, Field


class ContaminationInvestigationRequest(BaseModel):
    organization_id: UUID
    contaminated_node_id: UUID
    max_hops: int = Field(default=20, ge=1, le=100)


class GenealogyQuery(BaseModel):
    organization_id: UUID
    node_id: UUID
    direction: str = "downstream"
    max_hops: int = Field(default=20, ge=1, le=100)

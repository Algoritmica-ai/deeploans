from typing import Optional

from fastapi import APIRouter, Header, HTTPException

from app import big_query_useful as useful

router = APIRouter()


@router.get("/quality-summaries", tags=["etl"], name="etl_quality_summaries")
async def get_etl_quality_summaries(
    limit: int = None,
    offset: int = None,
    detailed: bool = True,
    x_algoritmica_api_key: Optional[str] = Header(None),  # needed for swagger
):
    try:
        params = dict(
            credit_type="sme",
            table_name="quality_summaries",
            filters=None,
            limit=limit,
            offset=offset,
            columns=None,
        )

        result = (
            await useful.query_to_list_of_dicts(**params)
            if detailed
            else await useful.query_to_list_of_lists(**params)
        )
        return result
    except Exception as ex:
        raise HTTPException(status_code=400, detail=str(ex))

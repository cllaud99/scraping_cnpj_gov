from pydantic import BaseModel, Field
from typing import List, Union, Annotated, Type, Optional
from pydantic import ValidationError
from datetime import datetime
import pandas as pd



class Empresas(BaseModel):
    CNPJ_BASICO: Optional[str]
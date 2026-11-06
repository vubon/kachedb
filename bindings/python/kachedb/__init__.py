"""
KacheDB Python Client & PyTorch/vLLM Zero-Copy Bindings.
"""

from .client import KacheClient
from .descriptor import TensorBlockDescriptor, TensorDType, TENSOR_DESCRIPTOR_MAGIC

__version__ = "0.3.2"
__all__ = ["KacheClient", "TensorBlockDescriptor", "TensorDType", "TENSOR_DESCRIPTOR_MAGIC"]

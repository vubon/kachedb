"""
KacheDB Python Client & PyTorch/vLLM Zero-Copy Bindings.
"""

from .client import KacheClient
from .descriptor import TENSOR_DESCRIPTOR_MAGIC, TensorBlockDescriptor, TensorDType

__version__ = "0.4.0"
__all__ = ["TENSOR_DESCRIPTOR_MAGIC", "KacheClient", "TensorBlockDescriptor", "TensorDType"]

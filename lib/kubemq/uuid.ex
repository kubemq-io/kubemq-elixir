defmodule KubeMQ.UUID do
  @moduledoc false

  @spec generate() :: String.t()
  def generate do
    bytes = :crypto.strong_rand_bytes(16)
    <<a::48, _version::4, b::12, _variant::2, c::62>> = bytes
    raw = <<a::48, 0b0100::4, b::12, 0b10::2, c::62>>
    encode(raw)
  end

  @spec ensure(String.t() | nil) :: String.t()
  def ensure(nil), do: generate()
  def ensure(""), do: generate()
  def ensure(id) when is_binary(id), do: id

  defp encode(
         <<a1::binary-size(4), a2::binary-size(2), a3::binary-size(2), a4::binary-size(2),
           a5::binary-size(6)>>
       ) do
    IO.iodata_to_binary([
      Base.encode16(a1, case: :lower),
      ?-,
      Base.encode16(a2, case: :lower),
      ?-,
      Base.encode16(a3, case: :lower),
      ?-,
      Base.encode16(a4, case: :lower),
      ?-,
      Base.encode16(a5, case: :lower)
    ])
  end
end

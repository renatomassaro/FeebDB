defmodule Feeb.DB.NoWriteTransactionError do
  defexception message: "needs an open write transaction"
end

defmodule Feeb.DB.CaptureError do
  defexception [:message]
end

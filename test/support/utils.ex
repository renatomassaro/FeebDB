defmodule Test.Utils do
  def date_diff_ms(date_a, date_b),
    do: DateTime.diff(date_a, date_b, :millisecond)
end

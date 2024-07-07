Test.Feeb.DB.on_start()

ExUnit.start()

ExUnit.after_suite(fn _ ->
  Test.Feeb.DB.on_finish()
end)

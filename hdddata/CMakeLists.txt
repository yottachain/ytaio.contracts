add_contract(hdddata hdddata ${CMAKE_CURRENT_SOURCE_DIR}/src/hdddata.cpp)
target_include_directories(hdddata.wasm
   PUBLIC
   ${CMAKE_CURRENT_SOURCE_DIR}/include)

set_target_properties(hdddata.wasm
   PROPERTIES
   RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")

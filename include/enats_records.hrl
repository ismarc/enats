-record(connect, {verbose=false,
                  pedantic=false,
                  user=undefined,
                  pass=undefined}).
-record(server_info, {server_id,
                      max_payload,
                      version,
                      auth_required}).

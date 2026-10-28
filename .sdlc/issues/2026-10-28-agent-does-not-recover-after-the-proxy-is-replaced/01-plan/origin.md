# [conversation] Agent does not recover after the proxy is replaced

<!-- BEGIN VERBATIM COPY — do not edit, reformat or translate -->

## Initial report

this is happening in n8n syncs: Problem in node ‘Insert order header‘

Connection lost - socket hang up  because dbridge is dying on the client take a look at the k8s logs from the server it says no agents available │ proxy_started host=0.0.0.0 agents_port=7000 clients_port=9000                                                                                                                            │
│ client=10.1.109.193:53146 state=connected                                                                                                                                                │
│ client=10.1.109.193:53146 no_agent_available max_attempts=10                                                                                                                             │  how can i understand whats really happening? because im connected via rdp and it says the service is running

## Follow-ups

you can do that reading of kubectl just look at ctx pro and seteras/jle both had that issue

Get-NetTCPConnection -RemoteAddress 10.0.21.21 -ErrorAction SilentlyContinue |
  Select LocalPort,RemotePort,State,CreationTime | Sort CreationTime returned nothing

okay i just restarted the service in the client and it worked

do sdlc but without the board part

<!-- END VERBATIM COPY -->

---

origin: conversation
repo: null
number: null
url: null
author: jpalma-setwin
created_at: 2026-10-28T11:43:00Z
fetched_at: 2026-10-28T11:59:04Z
labels: []

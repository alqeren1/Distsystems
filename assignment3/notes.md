-chat message utf-9 string max 128 chars

-also timestamp should be passed

-Participants may join the system at any time. When a new participant X joins, the service must broadcast a message of the form: "Participant X joined Chit Chat at logical time L". This message must be delivered to all participants, including the newly joined one.

-Participants may leave the system at any time. When a participant X leaves, the service must broadcast a message of the form: "Participant X left Chit Chat at logical time L". This message must be delivered to all remaining participants.

MESSAGES IN PROTO:

-join request
-leave request
-publish message
-broadcast message to all clients

"make proto" code for rebuilding proto

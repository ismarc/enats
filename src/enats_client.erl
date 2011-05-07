%%%-------------------------------------------------------------------
%%% File        : enats_client.erl
%%% Author      : Matthew Brace <ismarc31@gmail.com>
%%% Description : Module for providing NATS pub/sub functionality as 
%%%               a client in erlang
%%% Created     : May 02, 2011
%%%-------------------------------------------------------------------
-module(enats_client).

-include("../include/enats_records.hrl").
-include("../include/jsonerl.hrl").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

%% API
-export([start_link/0, connect/2, disconnect/0, receive_message/1, 
         send_ping/0, subscribe/2, unsubscribe/1, publish/2]).

%% gen_server callbacks
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, 
         code_change/3, terminate/2]).

-record(state, {subscriptions=[], user=undefined, pass=undefined, 
                pending_requests=[], sid=1}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok, Pid} | ignore | {error, Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% Function: connect({Address, Port}, {Username, Password}) -> none
%% Description: Establishes a connection to the specified server/port 
%%              and authenticates with the supplied username/password
%%              if authentication is requested from the server.
%%--------------------------------------------------------------------
connect({Address, Port}, {Username, Password}) ->
    gen_server:cast(?SERVER, {connect, {Address, Port}, 
                              {Username, Password}}).
%%--------------------------------------------------------------------
%% Function: disconnect() -> none
%% Description: Disconnect from the server
%%--------------------------------------------------------------------
disconnect() ->
    gen_server:cast(?SERVER, {disconnect}).

%%--------------------------------------------------------------------
%% Function: receive_message(Message) -> none
%% Description: Callback used for receiving messages that arrive over 
%%              the open connection.
%%--------------------------------------------------------------------
receive_message(Message) ->
    gen_server:cast(?SERVER, {receive_message, Message}).

%%--------------------------------------------------------------------
%% Function: send_ping() -> none
%% Description: Sends a ping request to the server.
%%--------------------------------------------------------------------
send_ping() ->
    gen_server:cast(?SERVER, {send_ping}).

%%--------------------------------------------------------------------
%% Function: subscribe(Subject, Callback) -> Sid
%%           Callback = fun(Subject, Data)
%% Description: Sends a subscription request to the server for the 
%%              supplied Subject.  When a message is published with 
%%              that subject, Callback will be executed.  Sid is the 
%%              unique subscription identifier used to reference the
%%              subscription.
%%--------------------------------------------------------------------
subscribe(Subscription, Callback) ->
    gen_server:call(?SERVER, {subscript, Subscription, Callback}).

%%--------------------------------------------------------------------
%% Function: unsubscribe(SubscriptionId) -> none
%% Description: Unsubscribe from the subscription referenced by the 
%%              supplied SubscriptionId.
%%--------------------------------------------------------------------
unsubscribe(Subscription) ->
    gen_server:cast(?SERVER, {unsubscribe, Subscription}).

%%--------------------------------------------------------------------
%% Function: publish(Subject, Message) -> none
%% Description: Publishes a message to the server with the supplied 
%%              subject and message.
%%--------------------------------------------------------------------
publish(Subject, Message) ->
    gen_server:cast(?SERVER, {publish, Subject, Message}).

%%====================================================================
%% gen_server callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Function: init(Config) -> {ok, State}
%% Description: Initializes the configuration to be used and passed in
%%              callbacks.
%%--------------------------------------------------------------------
init(_Config) ->
    enats_raw_connection:start_link(fun receive_message/1),
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% Function: handle_call(Message, From, State) -> {reply, Reply,
%%                                                 State}
%% Description: Handles a synchronous method call with a return
%%              result.
%%--------------------------------------------------------------------
handle_call({subscribe, Subscription, Callback}, _From, State) ->
    Message = io_lib:format("SUB ~s ~w\r\n", 
                            [Subscription, State#state.sid]),
    enats_raw_connection:send_message(Message),
    Reply = State#state.sid,
    {reply,
     Reply,
     State#state{subscriptions=[{Subscription, Callback, 
                                 State#state.sid}|
                                State#state.subscriptions],
                 pending_requests=[subscription|
                                   State#state.pending_requests],
                 sid=State#state.sid + 1}};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Message, State) -> {noreply, State}
%% Description: Handles an asynchronous (no return value) method call
%%--------------------------------------------------------------------
handle_cast({connect, {Address, Port}, 
             {Username, Password}}, State) ->
    enats_raw_connection:connect(Address, Port),
    {noreply, State#state{user=Username, pass=Password}};
handle_cast({disconnect}, State) ->
    enats_raw_connection:shutdown(),
    {noreply, State};
handle_cast({receive_message, Message}, State) ->
    NState = process_received_message(Message, State),
    {noreply, NState};
handle_cast({send_ping}, State) ->
    enats_raw_connection:send_message("PING\r\n"),
    {noreply, State};
handle_cast({unsubscribe, Subscription}, State) ->
    case lists:keyfind(Subscription, 3, State#state.subscriptions) of
        {_Sub, _Callback, Sid} ->
            Message = io_lib:format("UNSUB ~p\r\n", [Sid]),
            enats_raw_connection:send_message(Message),
            {noreply, 
             State#state{subscriptions=
                         lists:delete(
                           lists:keyfind(Subscription, 
                                         1, 
                                         State#state.subscriptions), 
                           State#state.subscriptions),
                         pending_requests=
                         [unsubscribe|State#state.pending_requests]}};
        _ ->
            {noreply, State}
    end;
handle_cast({publish, Subject, Message}, State) ->
    Bytes = byte_size(list_to_binary(Message)),
    Request = io_lib:format("PUB ~s ~w\r\n~s\r\n", 
                            [Subject, Bytes, Message]),
    enats_raw_connection:send_message(Request),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State}
%% Description: Handles all other messages
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> ok.
%% Description: Notifies the process that it will be terminated,
%%              allowing it to do any necessary cleanup.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Function: code_change(OldVersion, State, Extra) -> {ok, State}
%% Description: Notifies the process that there is a new version of
%%              code and should process any necessary upgrade steps.
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal API
process_received_message(Message, State) when length(Message) > 0->
    Header = string:sub_string(Message, 1, 
                               string:cspan(Message, "\r\n")),
    Rest = string:sub_string(Message, 
                             string:cspan(Message, "\r\n") + 3),

    case string:tokens(Header, " ") of
        ["INFO"|JsonRecord] ->
            Data = ?json_to_record(server_info, JsonRecord),
            process_received_message(
              Rest, 
              handle_auth(Data#server_info.auth_required, State));
        "PING" ->
            enats_raw_connection:send_message(
              io_lib:format("PONG\r\n")),
            process_received_message(Rest, State);
        ["MSG", Subject, Index, Size] ->
            Body = string:substr(Rest, 1, list_to_integer(Size) + 1),
            Remainder = string:substr(Rest, 
                                      list_to_integer(Size) + 3),
            process_received_message(
              Remainder, 
              execute_callback(Subject, Body, Index, State));
        ["+OK"] ->
            process_received_message(Rest, 
                                     handle_pending_request(State));
        ["-ERR"|_] ->
            io:format("Error received for pending action: ~s~n", 
                      [Header]),
            process_received_message(Rest, State);
        _ ->
            State
    end;
process_received_message(_, State) ->
    State.
                               
execute_callback(Subject, Body, Sid, State) ->
    case lists:keyfind(list_to_integer(Sid), 3, 
                       State#state.subscriptions) of
        {_FoundSubject, Callback, _Sid} ->
            Callback(Subject, Body),
            State;
        _ ->
            State
    end.

handle_auth(RequireAuth, State) ->
    case RequireAuth of
        true ->
            Connect = #connect{user=list_to_atom(State#state.user),
                               pass=list_to_atom(State#state.pass)},
            Json = ?record_to_json(connect, Connect),
            enats_raw_connection:send_message(
              io_lib:format("CONNECT ~s\r\n", [Json])),
            State#state{
              pending_requests=[authentication|
                                State#state.pending_requests]};
        _ ->
            State
    end.

handle_pending_request(State) ->
    case State#state.pending_requests of
        [_Completed, Pending] ->
            State#state{pending_requests=Pending};
        [_Completed] ->
            State#state{pending_requests=[]};
        [] ->
            State
    end.

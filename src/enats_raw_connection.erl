-module(enats_raw_connection).

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-export([start_link/1, connect/2, shutdown/0, send_message/1]).

%% gen_server callbacks
-export([init/1, handle_info/2, handle_cast/2, handle_call/3, 
         code_change/3, terminate/2]).

-record(state, {socket=undefined, callback=undefined}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok, Pid} | ignore | {error, Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Callback) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Callback], []).

connect(Address, Port) ->
    gen_server:cast(?SERVER, {connect, {Address, Port}}).

shutdown() ->
    gen_server:cast(?SERVER, {shutdown}).

send_message(Message) ->
    gen_server:cast(?SERVER, {send_message, Message}).

%%====================================================================
%% gen_server callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Function: init(Config) -> {ok, State}
%% Description: Initializes the configuration to be used and passed in
%%              callbacks.
%%--------------------------------------------------------------------
init([Callback|_Config]) ->
    {ok, #state{callback=Callback}}.

%%--------------------------------------------------------------------
%% Function: handle_call(Message, From, State) -> {reply, Reply,
%%                                                 State}
%% Description: Handles a synchronous method call with a return
%%              result.
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Message, State) -> {noreply, State}
%% Description: Handles an asynchronous (no return value) method call
%%--------------------------------------------------------------------
handle_cast({connect, {Address, Port}}, State) ->
    case State#state.socket of
        undefined ->
            {ok, NSocket} = gen_tcp:connect(Address, Port, []);
        Socket ->
            gen_tcp:close(Socket),
            {ok, NSocket} = gen_tcp:connect(Address, Port, [])
    end,
    {noreply, State#state{socket=NSocket}};
handle_cast({shutdown}, State) ->
    case State#state.socket of 
        undefined ->
            ok;
        Socket ->
            gen_tcp:close(Socket)
    end,
    {noreply, State#state{socket=undefined}};
handle_cast({send_message, Message}, State) ->
    ok = gen_tcp:send(State#state.socket, Message),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State}
%% Description: Handles all other messages
%%--------------------------------------------------------------------
handle_info({tcp, _Socket, Data}, State) ->
    case State#state.callback of
        undefined ->
            {noreply, State};
        Fun ->
            Fun(Data),
            {noreply, State}
    end;
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

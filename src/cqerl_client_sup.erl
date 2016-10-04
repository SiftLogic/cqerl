-module(cqerl_client_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         add_clients/2]).

%% Supervisor callbacks
-export([init/1]).

-define(DEFAULT_NUM_CLIENTS, 20).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, main).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(main) ->
    {ok, { {simple_one_for_one, 5, 10}, [ {key_sup, {supervisor, start_link, [?MODULE]},
                                          transient, 5000, supervisor, [?MODULE]}
                                        ]}};

init([key, Key = {Node, _Opts}, FullOpts, OptGetter, ChildCount]) ->
    {ok, { {one_for_one, 5, 10}, [
        client_spec(Key, Node, FullOpts, OptGetter, I) ||
        I <- lists:seq(1, ChildCount)
                                 ] }}.

client_spec(Key, Node, FullOpts, OptGetter, I) ->
    { {cqerl_client, Key, I}, {cqerl_client, start_link, [Node, FullOpts, OptGetter, Key]},
      permanent, brutal_kill, worker, [cqerl_client]}.

add_clients(Node, Opts) ->
    Key = cqerl_client:make_key(Node, Opts),
    ChildCount = child_count(Key),
    GlobalOpts = cqerl:get_global_opts(),
    OptGetter = cqerl:make_option_getter(Opts, GlobalOpts),
    FullOpts = [ {ssl, OptGetter(ssl)}, {keyspace, OptGetter(keyspace)} ],

    case supervisor:start_child(?MODULE, [[key, Key, FullOpts, OptGetter, ChildCount]]) of
        {ok, SupPid} -> {ok, {ChildCount, SupPid}};
        {error, E} -> {error, E}
    end.


child_count(_Key) ->
    application:get_env(cqerl, num_clients, 20).

Advanced topics
===============

Using the Snowflake deserializer
--------------------------------

``diepvries`` is bundled with a deserializer: it is able to extract
metadata from Snowflake in order to automatically instantiate Data
Vault entities. As long as tables follow the naming conventions,
:class:`~diepvries.hub.Hub`,
:class:`~diepvries.satellite.Satellite` and
:class:`~diepvries.link.Link` objects can be created
automatically.


When using the deserializer, a connection to your database will be
initiated, in order to query the metadata tables. This will provide
``diepvries`` with all the information required to automatically
create Data Vault entity objects.

Here's how to use the deserializer:

.. literalinclude:: snippets/deserializer.py
   :language: python

.. code-block:: shell

    % python3 deserializer.py
    [
      <diepvries.hub.Hub object at 0x7fbe3cc5baf0>,
      <diepvries.satellite.Satellite object at 0x7fbe3cc5be20>
    ]
    ['h_customer', 'hs_customer']

The main advantage of the deserializer is DRY: your Data Vault DDLs
can live anywhere, stored in your favourite format, and ``diepvries``
will extrapolate this information, so you don't have to describe the
tables in Python.

Not using the deserializer
--------------------------

It is also possible to use ``diepvries`` without the deserializer, if
you prefer to "manually" configure it. For example, you might have
your Data Vault tables configuration stored in JSON or YAML, and you
could use those files to instantiate Hubs, Links and
Satellites. Another potential reason is if you don't have access to
your database instance when ``diepvries`` is running, maybe you only
want to generate SQL and then forward the queries to another component
of your system.

In that case, you can instantiate everything manually:

.. literalinclude:: snippets/nodeserializer.py
   :language: python

Role-playing hubs
-----------------

Role-playing hubs are hubs not materialized as tables, but rather as
views pointing to a main hub. They are useful to model different roles
around the same concept. For example, on top of the account hub
``h_account``, different role-playing hubs could be
``h_account_supplier`` and ``h_account_transporter``, implemented as
views pointing to ``h_account``.

``diepvries`` can work with role-playing hubs. The deserializer
supports them:

.. literalinclude:: snippets/rph_deserializer.py
   :language: python

.. code-block:: shell

    % python3 rph_deserializer.py
    [
      <diepvries.hub.Hub object at 0x7fb83c455250>,
      <diepvries.role_playing_hub.RolePlayingHub object at 0x7fb83e7ae700>,
      <diepvries.role_playing_hub.RolePlayingHub object at 0x7fb83e7ae640>
    ]
    ['h_account', 'h_account_supplier', 'h_account_transporter']

Role-playing hubs can also be directly instantiated:

.. literalinclude:: snippets/rph.py
   :language: python

Effectivity satellites
----------------------

Effectivity satellites model links which can change over time. For
example, imagine a link between a Customer and a Contact entry. A
Customer can have multiple Contact entries, however it could be
required to keep only one Contact entry per Customer open at a given
point in time. This means that, if a Customer changes its Contact
entry, only the latest relationship between the Customer and its
Contact is kept as an open relationship.

This is also supported by ``diepvries``:

.. literalinclude:: snippets/effsat_deserializer.py
   :language: python

.. code-block:: shell

    % python3 effsat_deserializer.py
    [
      <diepvries.link.Link object at 0x7fd64db22520>,
      <diepvries.effectivity_satellite.EffectivitySatellite object at 0x7fd64db22ac0>
    ]
    ['l_foo_bar', 'ls_foo_bar_eff']

Effectivity satellites can also be directly instantiated:

.. literalinclude:: snippets/effsat.py
   :language: python

{-# LANGUAGE QuasiQuotes, ExtendedDefaultRules #-}
module DB.PG ( module DB
             , module DB.PG
             ) where

import Database.PostgreSQL.Simple
import Data.ByteString (ByteString)
import Data.Proxy
import Data.String (IsString(..))
import GHC.TypeLits
import qualified Data.Text as Text
import Text.InterpolatedString.Perl6 (qc)

import DB

class PostgreSQLEngineAttributes a where
  connectionString :: a -> ByteString

instance PostgreSQLEngineAttributes String where
  connectionString = fromString

-- DAL/PG
newtype PostgreSQLEngine = PostgreSQLEngine { conn :: Connection }

instance Monad m => HasConnection PostgreSQLEngine m where
  type DbConnection PostgreSQLEngine = Connection
  getConnection eng = pure $ conn eng

instance HasTransaction PostgreSQLEngine IO where
  transaction eng action = do
    conn <- getConnection eng
    withTransaction conn action


createEngine :: PostgreSQLEngineAttributes a => a -> IO PostgreSQLEngine
createEngine att = PostgreSQLEngine <$> connectPostgreSQL (connectionString att)

disposeEngine :: PostgreSQLEngine -> IO ()
disposeEngine e = do
  conn <- getConnection e
  close conn


data From (table :: Symbol) cols = All
data Rows cols

type family SelectRowType a :: *
type instance SelectRowType (From t (Rows [(a1,a2)])) = (a1,a2)
type instance SelectRowType (From t (Rows [(a1,a2,a3)])) = (a1,a2,a3)
type instance SelectRowType (From t (Rows [(a1,a2,a3,a4)])) = (a1,a2,a3,a4)

instance ( HasTable   (From t (Rows [row])) PostgreSQLEngine
         , HasColumns (From t (Rows [row]))
         , FromRow row
         ) => SelectStatement (From t (Rows [row])) IO PostgreSQLEngine
      where
        type SelectResult (From t (Rows [row])) = [row]
        select eng q = do
          conn <- getConnection eng
          query_ conn [qc|select {cols} from {table}|]
          where table = tablename eng q
                cols  = Text.intercalate "," (columns q)

instance ( HasColumn t (Proxy a)
         , HasColumn t (Proxy b)
         , KnownSymbol t
         ) => HasColumns (From t (Rows [(a, b)])) where
  columns = const [ column (Proxy @t) (Proxy @a), column (Proxy @t) (Proxy @b) ]


instance ( HasColumn t (Proxy a1)
         , HasColumn t (Proxy a2)
         , HasColumn t (Proxy a3)
         , KnownSymbol t
         ) => HasColumns (From t (Rows [(a1, a2, a3)])) where

  columns = const [ column (Proxy @t) (Proxy @a1)
                  , column (Proxy @t) (Proxy @a2)
                  , column (Proxy @t) (Proxy @a3)
                  ]


instance ( HasColumn t (Proxy a1)
         , HasColumn t (Proxy a2)
         , HasColumn t (Proxy a3)
         , HasColumn t (Proxy a4)
         , KnownSymbol t
         ) => HasColumns (From t (Rows [(a1, a2, a3, a4)])) where

  columns = const [ column (Proxy @t) (Proxy @a1)
                  , column (Proxy @t) (Proxy @a2)
                  , column (Proxy @t) (Proxy @a3)
                  , column (Proxy @t) (Proxy @a4)
                  ]



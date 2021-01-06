{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE QuasiQuotes, ExtendedDefaultRules #-}
module DB.PG.Instances where

import Control.Newtype as Newtype
import Database.PostgreSQL.Simple
import Database.PostgreSQL.Simple.FromField
import Database.PostgreSQL.Simple.FromRow
import Database.PostgreSQL.Simple.ToField
import Data.Data
import Data.Proxy
import Data.Scientific
import Data.String
import Data.Text.Conversions
import Data.Text (Text)
import Data.Time.Calendar
import GHC.Generics
import GHC.TypeLits (KnownSymbol(..), symbolVal)
import qualified Data.Text as Text
import qualified GHC.TypeLits as T
import Text.InterpolatedString.Perl6 (qc)

import DB.PG
import Data.MOEX

instance ToText Day where
  toText d = toText (show d)

instance FromRow Symbol where
  fromRow = Symbol <$> field

instance ToField Symbol where
  toField x = toField (Newtype.unpack x)

instance FromField Symbol where
  fromField f x = Symbol <$> fromField f x

instance FromField Volume where
  fromField f x = (Volume <$> realToFrac) <$> (fromField @Scientific f x)

instance FromField BondCouponFreq where
  fromField f x = BondCouponFreq <$> fromField f x

instance FromField BondCouponValue where
  fromField f x = (BondCouponValue <$> realToFrac) <$> (fromField @Scientific f x)

instance FromField BondIssueDate where
  fromField f x = BondIssueDate <$> fromField f x

instance FromField BondMatDate where
  fromField f x = BondMatDate <$> fromField f x

instance FromField BondShortName where
  fromField f x = BondShortName <$> fromField f x

instance FromField BondFullName where
  fromField f x = BondFullName <$> fromField f x

instance FromField TradeAggDate where
  fromField f x = TradeAggDate <$> fromField f x

instance FromField TradeAggVol where
  fromField f x = TradeAggVol <$> fromField f x

instance ToField Market where
  toField (Market x) = toField x

instance ToField Price where
  toField (Price x) = toField (realToFrac x :: Scientific)

instance ToField Volume where
  toField (Volume x) = toField (realToFrac x :: Scientific)

instance HasColumn "bondshortname" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondshortname" (Proxy BondShortName) where
  column _ _ = "value"

instance HasColumn "bondfullname" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondfullname" (Proxy BondFullName) where
  column _ _ = "value"

instance HasColumn "bondtradeagg" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondcouponfreq" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondcouponfreq" (Proxy BondCouponFreq) where
  column _ _ = "value"

instance HasColumn "bondissuedate" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondissuedate" (Proxy BondIssueDate) where
  column _ _ = "value"

instance HasColumn "bondmatdate" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondmatdate" (Proxy BondMatDate) where
  column _ _ = "value"

instance HasColumn "bondcouponvalue" (Proxy Symbol) where
  column _ _ = "symbol"

instance HasColumn "bondcouponvalue" (Proxy BondCouponValue) where
  column _ _ = "value"

newtype TradeAggDate = TradeAggDate Day
                       deriving (Eq,Ord,Show,Data,Generic)

instance Newtype TradeAggDate Day

instance ToText TradeAggDate where
  toText = toText . Newtype.unpack

newtype TradeAggVol  = TradeAggVol (Maybe Scientific)
                       deriving (Eq,Ord,Show,Data,Generic)

instance Newtype TradeAggVol (Maybe Scientific)

instance ToText TradeAggVol where
  toText (TradeAggVol (Just x))  = toText (show x)
  toText (TradeAggVol Nothing) = ""

newtype TradeAggNum  = TradeAggNum (Maybe Int)
                       deriving (Eq,Ord,Show,Data,Generic)

instance Newtype TradeAggNum (Maybe Int)

instance ToText TradeAggNum where
  toText (TradeAggNum (Just x)) = toText (show x)
  toText (TradeAggNum Nothing)  = ""

instance HasColumn "bondtradeagg" (Proxy TradeAggDate) where
  column _ _  = "day"

instance HasColumn "bondtradeagg" (Proxy TradeAggVol) where
  column _ _  = "vol"

instance HasColumn "bondtradeagg" (Proxy TradeAggNum) where
  column _ _  = "trades"

instance HasColumn "vbonddate" (Proxy Symbol) where
  column _ _  = "symbol"

instance HasColumn "vbonddate" (Proxy BondIssueDate) where
  column _ _  = "issuedate"

instance HasColumn "vbonddate" (Proxy BondMatDate) where
  column _ _  = "matdate"

instance HasColumn "vbonddate" (Proxy BondCouponFreq) where
  column _ _  = "freq"

instance HasColumn "vbonddate" (Proxy BondCouponValue) where
  column _ _  = "coupon"

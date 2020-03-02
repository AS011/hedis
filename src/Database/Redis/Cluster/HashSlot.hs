{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
module Database.Redis.Cluster.HashSlot(HashSlot, keyToSlot) where

import Data.Bits((.&.))
import qualified Data.ByteString.Char8 as Char8
import qualified Data.ByteString as BS
import Data.Word(Word8,Word16)
import Data.Bits

newtype HashSlot = HashSlot Word16 deriving (Num, Eq, Ord, Real, Enum, Integral, Show)

numHashSlots :: Word16
numHashSlots = 16384

-- | Compute the hashslot associated with a key
keyToSlot :: BS.ByteString -> HashSlot
keyToSlot = HashSlot . (.&.) (numHashSlots - 1) . crc16 . findSubKey

-- | Find the section of a key to compute the slot for.
findSubKey :: BS.ByteString -> BS.ByteString
findSubKey key = case Char8.break (=='{') key of
  (whole, "") -> whole
  (_, xs) -> case Char8.break (=='}') (Char8.tail xs) of
    ("", _) -> key
    (subKey, _) -> subKey

crc16 :: BS.ByteString -> Word16
crc16 = BS.foldl (crc16_update 0x1021 False) 0

crc16_update :: Word16      -- ^ polynomial
             -> Bool        -- ^ inverse bits 
             -> Word16      -- ^ initial crc
             -> Word8       -- ^ data byte
             -> Word16      -- ^ new crc
crc16_update poly rev crc b =
    foldl (crc16_update_bit poly) new_crc [1..(bitSize b)]
    where
        new_crc = crc `xor` (shiftL (fromIntegral b' :: Word16) 8 )
        b' = if rev
                then reverse_bits b
                else b




crc16_update_bit :: Word16 -> Word16 -> Int -> Word16
crc16_update_bit poly crc _ =
    if (crc .&. 0x8000) /= 0x0000
        then (shiftL crc 1) `xor` poly 
        else shiftL crc 1



-- | Reverse the bits in a byte
--
-- 7..0 becomes 0..7
--
reverse_bits :: Word8 -> Word8
reverse_bits b =
    (shiftL (b .&. 0x01) 7) .|. (shiftL (b .&. 0x02) 5) .|. (shiftL (b .&. 0x04) 3) .|. (shiftL (b .&. 0x08) 1) .|.
    (shiftR (b .&. 0x10) 1) .|. (shiftR (b .&. 0x20) 3) .|. (shiftR (b .&. 0x40) 5) .|. (shiftR (b .&. 0x80) 7)

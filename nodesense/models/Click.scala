package ai.nodesense.models


import org.joda.time.DateTime

case class Click(sessionId: Int,
                 timeStamp: DateTime,
                 itemId: Int,
                 category: Int
                );

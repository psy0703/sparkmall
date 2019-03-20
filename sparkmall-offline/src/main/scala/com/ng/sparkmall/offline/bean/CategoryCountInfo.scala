package com.ng.sparkmall.offline.bean

case class CategoryCountInfo(taskId: String,
                             categoryId: String,
                             clickCount: Long,
                             orderCount: Long,
                             payCount: Long)

<template>
<div class="app-container" v-show="orderNumber !== '' && orderNumber !== null">
  <Header></Header>
  <div class="main">
    <el-icon :size="50" class="iconCircle"><CircleCheck   color="rgb(255, 40, 105)" /></el-icon>
    <span class="paySuccess">{{ resultText }}</span>
   <div class="btn">
     <el-button  class="continueQuery" @click="continueQuery"    >继续逛逛</el-button>
     <el-button   class="orderQuery" @click="orderQuery"    >订单列表</el-button>
   </div>
  </div>
  <Footer></Footer>
</div>
</template>

<script setup name="PaySuccess">
import Header from '@/components/header/index'
import Footer from '@/components/footer/index'
import {ref, nextTick, onActivated, onMounted} from 'vue'
import {useRoute, useRouter} from 'vue-router'
import {payCheckApi} from '@/api/order.js'
const route = useRoute();
const router = useRouter();
const orderNumber = ref('');
const resultText = ref('正在确认支付结果...')

//继续逛逛
const  continueQuery=()=>{
  router.replace({path:'/index'})
}
//查看订单列表
const orderQuery=()=>{
  router.push({path:'/orderManagement/index'})
}

onMounted(async () => {
  orderNumber.value =
    localStorage.getItem('orderNumber')

  console.log(
    '这里是获取的订单号================',
    orderNumber.value
  )

  if (!orderNumber.value) {
    router.replace({ path: '/' })
    return
  }

  try {
    const response = await payCheckApi({
      orderNumber: orderNumber.value,
      payChannelType: 1
    })

    console.log(
      '交易状态查询结果',
      response.data
    )

    const orderStatus =
      Number(response?.data?.orderStatus)

    switch (orderStatus) {
      case 1:
        resultText.value =
          '支付结果确认中，请稍后查看订单'
        break

      case 2:
        resultText.value =
          '订单已关闭'
        localStorage.removeItem('orderNumber')
        break

      case 3:
        resultText.value =
          '支付成功'
        localStorage.removeItem('orderNumber')
        break

      case 4:
        resultText.value =
          '订单已退款'
        localStorage.removeItem('orderNumber')
        break

      default:
        resultText.value =
          '暂时无法确认支付结果'
    }
  } catch (error) {
    console.error(
      '支付状态查询失败',
      error
    )

    resultText.value =
      '支付状态查询失败，请稍后查看订单'
  }
})
</script>

<style scoped lang="scss">
.app-container{
  width: 1200px;
  margin: 0 auto;
  overflow: auto;

  .main{
    height: 500px;
    width: 100%;
    //background: red
    margin: 0 auto;
    padding-top: 100px;
    text-align: center;
    position: relative;
    .iconCircle{
      position: absolute;
      margin-left: -60px;
      top: 95px;
      }
    .paySuccess{
        font-size: 30px;
       font-weight: bolder;

    }
    .btn{
      text-align: center;
      margin-top: 30px;
      .continueQuery{
        width: 100px;
        height: 30px;
        border-radius: 50px;
        &:hover{
          color: rgba(255, 55, 29, 0.85);
          border-color:  rgba(255, 55, 29, 0.85);
          background: #ffffff;
        }
      }
      .orderQuery{
        width: 100px;
        height: 30px;
        border-radius: 50px;
        &:hover{
          color: rgba(255, 55, 29, 0.85);
          border-color:  rgba(255, 55, 29, 0.85);
          background: #ffffff;
        }
      }
    }
  }
}
</style>
